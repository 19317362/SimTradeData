# -*- coding: utf-8 -*-
"""
5-minute K-line data download program

This program downloads 5-minute K-line data from BaoStock API with:
1. Progress tracking via JSON file (supports resume)
2. API call limit control (default: 100,000 calls/day)
3. Independent storage in ptrade_data_5m.h5
4. Multiple run support to complete full download
5. Multi-threaded download for improved performance

Note: 5-minute data only available from 2019-01-02.
"""

import argparse
import sys
from pathlib import Path

# Add project root to path for direct script execution
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import json
import logging
import time
import warnings
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, local
from queue import Queue
import threading

import baostock as bs
import pandas as pd
from tables import NaturalNameWarning
from tqdm import tqdm

# Monkey-patch baostock for pandas 2.x compatibility
# baostock uses DataFrame.append() which was removed in pandas 2.0
def _patched_get_data(self):
    if self.data is None:
        return pd.DataFrame()
    if len(self.data) == 0:
        return pd.DataFrame(columns=self.fields)
    return pd.DataFrame(self.data, columns=self.fields)

bs.data.resultset.ResultData.get_data = _patched_get_data


from simtradedata.fetchers.unified_fetcher import UnifiedDataFetcher
from simtradedata.writers.h5_writer import HDF5Writer
from simtradedata.utils.code_utils import convert_to_ptrade_code

warnings.filterwarnings("ignore", category=NaturalNameWarning)

# Configuration
OUTPUT_DIR = "data"
PROGRESS_FILE = "data/5m_progress.json"
LOG_FILE = "data/download_5m.log"

# 5-minute data specific configuration
KLINE_5M_START_DATE = "2019-01-02"  # BaoStock 5-minute data earliest date
KLINE_5M_END_DATE = None  # None means current date

# API limit configuration
DAILY_API_LIMIT = 100000  # BaoStock daily API limit
SAFE_API_THRESHOLD = 90000  # Stop before hitting limit

# Batch configuration
BATCH_SIZE = 50  # Stocks per batch for progress saving

# Thread configuration
DEFAULT_WORKERS = 1  # Fixed to 1 due to server-side rate limiting
MAX_WORKERS = 1  # Fixed to 1 due to server-side rate limiting

# Check and fill configuration
DEFAULT_TARGET_DATE = "2025-03-10"  # Default target date for --check-and-fill
DEFAULT_LOOP_INTERVAL = 60  # Default interval between loop iterations (seconds)

# Ensure data directory exists before logging setup
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# Logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",  # Append mode for multiple runs
)
logger = logging.getLogger(__name__)

# Also log to console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)
logger.addHandler(console_handler)


class APICounter:
    """Thread-safe API call counter with daily limit tracking"""

    def __init__(self, daily_limit: int = DAILY_API_LIMIT):
        self.daily_limit = daily_limit
        self.calls_today = 0
        self.calls_date = None
        self._lock = Lock()

    def check_limit(self, threshold: int = None) -> bool:
        """
        Check if API calls are below limit (thread-safe)

        Args:
            threshold: Custom threshold (default: SAFE_API_THRESHOLD)

        Returns:
            True if below limit, False otherwise
        """
        threshold = threshold or SAFE_API_THRESHOLD
        today = date.today()

        with self._lock:
            # Reset counter if new day
            if self.calls_date != today:
                self.calls_today = 0
                self.calls_date = today

            return self.calls_today < threshold

    def increment(self, count: int = 1) -> None:
        """Increment API call counter (thread-safe)"""
        with self._lock:
            self.calls_today += count

    def get_remaining(self, threshold: int = None) -> int:
        """Get remaining API calls before threshold (thread-safe)"""
        threshold = threshold or SAFE_API_THRESHOLD
        with self._lock:
            return max(0, threshold - self.calls_today)

    def to_dict(self) -> dict:
        """Export counter state (thread-safe)"""
        with self._lock:
            return {
                "api_calls_today": self.calls_today,
                "api_calls_date": str(self.calls_date) if self.calls_date else None,
            }

    def from_dict(self, data: dict) -> None:
        """Import counter state (thread-safe)"""
        with self._lock:
            self.calls_today = data.get("api_calls_today", 0)
            date_str = data.get("api_calls_date")
            if date_str and date_str != "None":
                try:
                    self.calls_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                except ValueError:
                    self.calls_date = None
            else:
                self.calls_date = None


class ThreadLocalBaoStock:
    """Thread-local BaoStock connection manager"""

    def __init__(self):
        self._local = local()
        self._lock = Lock()
        self._connections = {}  # Track connections by thread id

    def get_connection(self):
        """Get or create thread-local BaoStock connection"""
        thread_id = threading.current_thread().ident

        if not hasattr(self._local, 'logged_in') or not self._local.logged_in:
            lg = bs.login()
            if lg.error_code != '0':
                raise RuntimeError(f"BaoStock login failed: {lg.error_msg}")
            self._local.logged_in = True
            with self._lock:
                self._connections[thread_id] = True
            logger.debug(f"Thread {thread_id} logged in to BaoStock")

        return bs

    def logout_all(self):
        """Logout all thread connections"""
        with self._lock:
            for thread_id in list(self._connections.keys()):
                try:
                    bs.logout()
                except:
                    pass
            self._connections.clear()


class WriteTask:
    """Task for background writer thread"""
    def __init__(self, symbol: str, df: pd.DataFrame, result_type: str):
        self.symbol = symbol
        self.df = df
        self.result_type = result_type


class BackgroundWriter:
    """Background thread for HDF5 writing (producer-consumer pattern)"""

    def __init__(self, writer: 'HDF5Writer'):
        self.writer = writer
        self.queue = Queue()
        self._stop_event = threading.Event()
        self._thread = None
        self.write_count = 0
        self.error_count = 0

    def start(self):
        """Start background writer thread"""
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._writer_loop, daemon=True)
        self._thread.start()
        logger.info("Background writer thread started")

    def stop(self):
        """Stop background writer thread and wait for queue to drain"""
        self._stop_event.set()
        # Add sentinel to unblock queue.get()
        self.queue.put(None)
        if self._thread:
            self._thread.join(timeout=30)
        logger.info(f"Background writer stopped. Writes: {self.write_count}, Errors: {self.error_count}")

    def submit(self, task: WriteTask):
        """Submit a write task to the queue"""
        self.queue.put(task)

    def _writer_loop(self):
        """Main loop for background writer"""
        while not self._stop_event.is_set() or not self.queue.empty():
            try:
                task = self.queue.get(timeout=1)
                if task is None:  # Sentinel value
                    break

                try:
                    if task.df is not None and not task.df.empty:
                        self.writer.write_5m_kline_data(task.symbol, task.df, mode="a")
                        self.write_count += 1
                except Exception as e:
                    logger.error(f"Error writing {task.symbol}: {e}")
                    self.error_count += 1

                self.queue.task_done()
            except:
                # Queue.get timeout, continue loop
                pass

    def wait_for_completion(self):
        """Wait for all pending writes to complete"""
        self.queue.join()


class FiveMinuteKlineDownloader:
    """5-minute K-line data downloader with progress tracking and multi-threading"""

    def __init__(
        self,
        output_dir: str = OUTPUT_DIR,
        progress_file: str = PROGRESS_FILE,
        num_workers: int = DEFAULT_WORKERS,
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.progress_file = Path(progress_file)
        self.num_workers = min(num_workers, MAX_WORKERS)

        # Initialize components
        self.fetcher = UnifiedDataFetcher()
        self.writer = HDF5Writer(output_dir=output_dir)

        # Background writer (producer-consumer pattern, no locks needed for HDF5)
        self._bg_writer = None

        # Thread-safe lock for progress only
        self._progress_lock = Lock()
        self._stop_event = threading.Event()

        # Thread-local BaoStock connections
        self._thread_local_bs = ThreadLocalBaoStock()

        # API counter (already thread-safe)
        self.api_counter = APICounter()

        # Load progress
        self.progress = self._load_progress()

        # Restore API counter state
        self.api_counter.from_dict(self.progress)

        # Track failures in current session (thread-safe)
        self.session_failed = []
        self._failed_lock = Lock()

    def _load_progress(self) -> dict:
        """Load download progress from JSON file"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, "r", encoding="utf-8") as f:
                    progress = json.load(f)
                    logger.info(f"Loaded progress: {len(progress.get('completed_stocks', []))} completed")
                    # Migrate old progress files without completed_end_date
                    if "completed_end_date" not in progress or progress["completed_end_date"] is None:
                        # Try to detect from H5 file
                        detected_date = self.writer.get_5m_max_date(sample_size=20)
                        if detected_date:
                            progress["completed_end_date"] = detected_date
                            logger.info(f"Detected completed_end_date from H5: {detected_date}")
                        else:
                            progress["completed_end_date"] = None
                    # Migrate: add uptodate tracking fields if missing
                    if "uptodate_check_date" not in progress:
                        progress["uptodate_check_date"] = None
                        progress["uptodate_stocks"] = []
                    return progress
            except Exception as e:
                logger.error(f"Error loading progress file: {e}")

        # Default progress structure
        return {
            "version": 2,
            "start_date": KLINE_5M_START_DATE,
            "end_date": datetime.now().strftime("%Y-%m-%d"),
            "completed_end_date": None,  # Actual end date when stocks were completed
            "last_update": None,
            "total_stocks": 0,
            "completed_stocks": [],
            "failed_stocks": [],
            "api_calls_today": 0,
            "api_calls_date": None,
            "uptodate_check_date": None,  # Date when uptodate_stocks was last checked
            "uptodate_stocks": [],  # Stocks confirmed up-to-date on check_date
        }

    def _save_progress(self) -> None:
        """Save download progress to JSON file (thread-safe)"""
        with self._progress_lock:
            self.progress["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.progress.update(self.api_counter.to_dict())

            try:
                with open(self.progress_file, "w", encoding="utf-8") as f:
                    json.dump(self.progress, f, ensure_ascii=False, indent=2)
                logger.info(f"Progress saved: {len(self.progress['completed_stocks'])} completed")
            except Exception as e:
                logger.error(f"Error saving progress: {e}")

    def download_stock_5m_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        use_thread_local: bool = False,
    ) -> tuple:
        """
        Download 5-minute K-line data for a single stock

        Args:
            symbol: Stock code in PTrade format
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            use_thread_local: Use thread-local BaoStock connection

        Returns:
            Tuple of (result_code, dataframe)
            result_code: "success", "no_data", "failed", "stopped"
            dataframe: Downloaded data or None
        """
        try:
            # Check if stop requested
            if self._stop_event.is_set():
                return ("stopped", None)

            # Use thread-local connection if in multi-threaded mode
            if use_thread_local:
                self._thread_local_bs.get_connection()

            # Fetch data
            df = self.fetcher.fetch_5m_kline_data(symbol, start_date, end_date)

            # Increment API counter
            self.api_counter.increment()

            if df.empty:
                logger.info(f"No new 5m data for {symbol} ({start_date} ~ {end_date})")
                return ("no_data", None)

            logger.info(f"Downloaded 5m data for {symbol}: {len(df)} rows")
            return ("success", df)

        except Exception as e:
            logger.error(f"Failed to download {symbol}: {e}")
            with self._failed_lock:
                self.session_failed.append(symbol)
            return ("failed", None)

    def get_stock_pool(self, end_date: str) -> list:
        """
        Get all stock codes from BaoStock

        Args:
            end_date: Reference date for stock list

        Returns:
            List of stock codes in PTrade format
        """
        print("Getting stock pool...")

        # Sample multiple dates to get complete stock list
        sample_dates = pd.date_range(
            start=KLINE_5M_START_DATE,
            end=end_date,
            freq="QS"  # Quarterly start
        ).strftime("%Y-%m-%d").tolist()

        # Add end date if not in list
        if end_date not in sample_dates:
            sample_dates.append(end_date)

        all_stocks = set()

        for date_str in tqdm(sample_dates, desc="Sampling stock pool"):
            try:
                rs = bs.query_all_stock(day=date_str)
                if rs.error_code == "0":
                    stocks_df = rs.get_data()
                    if not stocks_df.empty:
                        ptrade_codes = [
                            convert_to_ptrade_code(code, "baostock")
                            for code in stocks_df["code"].tolist()
                        ]
                        all_stocks.update(ptrade_codes)
                        self.api_counter.increment()
            except Exception as e:
                logger.error(f"Error getting stock list for {date_str}: {e}")

        stock_pool = sorted(list(all_stocks))
        print(f"  Total stocks in pool: {len(stock_pool)}")

        return stock_pool

    def _process_single_stock(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        api_threshold: int,
    ) -> tuple:
        """
        Process a single stock for multi-threaded download

        Args:
            symbol: Stock code
            start_date: Start date for full download
            end_date: End date
            api_threshold: API call threshold

        Returns:
            Tuple of (symbol, result_type, dataframe)
            result_type: "success", "incremental", "no_data", "uptodate", "skipped_today", "failed", "stopped"
            dataframe: Downloaded data or None
        """
        # Check if stop requested
        if self._stop_event.is_set():
            return (symbol, "stopped", None)

        # Check API limit
        if not self.api_counter.check_limit(api_threshold):
            self._stop_event.set()
            return (symbol, "stopped", None)

        # Skip stocks already checked today with no new data (thread-safe check)
        with self._progress_lock:
            if symbol in self.progress.get("uptodate_stocks", []):
                return (symbol, "skipped_today", None)

        # Check stock's actual max date in H5 file
        # Note: This read is safe because only the background writer writes to H5
        stock_max_date = self.writer.get_5m_stock_max_date(symbol)

        if stock_max_date:
            if stock_max_date >= end_date:
                # Stock is already up to date
                return (symbol, "uptodate", None)
            else:
                # Need incremental download
                download_start = (
                    datetime.strptime(stock_max_date, "%Y-%m-%d")
                    + pd.Timedelta(days=1)
                ).strftime("%Y-%m-%d")

                result_code, df = self.download_stock_5m_data(
                    symbol, download_start, end_date, use_thread_local=True
                )

                if result_code == "success":
                    return (symbol, "incremental", df)
                elif result_code == "no_data":
                    return (symbol, "no_data", None)
                else:
                    return (symbol, "failed", None)
        else:
            # Stock not in H5, need full download
            result_code, df = self.download_stock_5m_data(
                symbol, start_date, end_date, use_thread_local=True
            )

            if result_code == "success":
                return (symbol, "success", df)
            elif result_code == "no_data":
                return (symbol, "no_data", None)
            else:
                return (symbol, "failed", None)

    def _update_progress_from_result(
        self,
        symbol: str,
        result_type: str,
    ) -> str:
        """
        Update progress based on download result (thread-safe)

        Returns:
            Category for statistics: "new", "incremental", "uptodate", "skipped", "no_data", "failed"
        """
        with self._progress_lock:
            if result_type == "success":
                if symbol not in self.progress["completed_stocks"]:
                    self.progress["completed_stocks"].append(symbol)
                if symbol in self.progress["failed_stocks"]:
                    self.progress["failed_stocks"].remove(symbol)
                return "new"

            elif result_type == "incremental":
                if symbol not in self.progress["completed_stocks"]:
                    self.progress["completed_stocks"].append(symbol)
                if symbol in self.progress["failed_stocks"]:
                    self.progress["failed_stocks"].remove(symbol)
                return "incremental"

            elif result_type == "uptodate":
                if symbol not in self.progress["completed_stocks"]:
                    self.progress["completed_stocks"].append(symbol)
                return "uptodate"

            elif result_type == "no_data":
                # uptodate_stocks set 已经在 progress 里，不需要额外维护 set
                if symbol not in self.progress["uptodate_stocks"]:
                    self.progress["uptodate_stocks"].append(symbol)
                if symbol not in self.progress["completed_stocks"]:
                    self.progress["completed_stocks"].append(symbol)
                return "no_data"

            elif result_type == "skipped_today":
                return "skipped"

            elif result_type == "failed":
                if symbol not in self.progress["failed_stocks"]:
                    self.progress["failed_stocks"].append(symbol)
                return "failed"

            else:  # stopped
                return "stopped"

    def download_all(
        self,
        max_stocks: int = None,
        max_api_calls: int = None,
        resume: bool = True,
        retry_failed: bool = False,
        start_date: str = None,
        end_date: str = None,
        use_threading: bool = True,
    ) -> dict:
        """
        Download 5-minute K-line data for all stocks

        Args:
            max_stocks: Maximum stocks to download this session
            max_api_calls: Maximum API calls this session
            resume: Whether to resume from previous progress
            retry_failed: Whether to retry previously failed stocks
            start_date: Override start date
            end_date: Override end date
            use_threading: Use multi-threaded download (default: True)

        Returns:
            Summary dict with download statistics
        """
        # Determine date range
        start_date = start_date or self.progress.get("start_date", KLINE_5M_START_DATE)
        end_date = end_date or datetime.now().strftime("%Y-%m-%d")

        # Update progress dates
        self.progress["start_date"] = start_date
        self.progress["end_date"] = end_date

        # Set API call limit for this session
        api_threshold = max_api_calls or SAFE_API_THRESHOLD

        print("=" * 70)
        print("5-Minute K-line Data Download Program")
        print("=" * 70)
        print(f"Date range: {start_date} ~ {end_date}")
        print(f"API call limit: {api_threshold}")
        if max_stocks:
            print(f"Max stocks this session: {max_stocks}")
        if use_threading:
            print(f"Multi-threaded mode: {self.num_workers} workers")
        else:
            print("Single-threaded mode")
        print("=" * 70)

        # Login to BaoStock
        self.fetcher.login()

        try:
            # Get stock pool
            stock_pool = self.get_stock_pool(end_date)
            self.progress["total_stocks"] = len(stock_pool)

            # Determine stocks to download
            completed = set(self.progress.get("completed_stocks", []))
            failed = set(self.progress.get("failed_stocks", []))

            if retry_failed:
                # Only retry failed stocks
                stocks_to_process = sorted(list(failed))
                print(f"\nRetrying {len(stocks_to_process)} failed stocks...")
            elif resume:
                # Process all stocks (check each one's actual max date)
                stocks_to_process = stock_pool
                print(f"\nResume mode: will check each stock's actual data range")
                print(f"  Previously marked completed: {len(completed)}")
                print(f"  Previously marked failed: {len(failed)}")
            else:
                # Full download (ignore previous progress)
                stocks_to_process = stock_pool
                self.progress["completed_stocks"] = []  # Reset
                print(f"\nFull download mode: {len(stocks_to_process)} stocks")

            if not stocks_to_process:
                print("\nNo stocks to process!")
                return {"success": 0, "failed": 0, "skipped": 0}

            # Apply max_stocks limit
            if max_stocks and len(stocks_to_process) > max_stocks:
                stocks_to_process = stocks_to_process[:max_stocks]
                print(f"Limited to {max_stocks} stocks this session")

            # Handle uptodate_stocks tracking (stocks checked today with no new data)
            today = datetime.now().strftime("%Y-%m-%d")
            uptodate_check_date = self.progress.get("uptodate_check_date")
            if uptodate_check_date != today:
                # New day - reset uptodate_stocks list
                self.progress["uptodate_check_date"] = today
                self.progress["uptodate_stocks"] = []
                logger.info(f"New check date {today}, reset uptodate_stocks list")

            # Get initial count for display (progress["uptodate_stocks"] is the source of truth)
            initial_uptodate_count = len(self.progress.get("uptodate_stocks", []))

            # Download statistics
            new_downloads = 0
            incremental_updates = 0
            skipped_uptodate = 0
            skipped_checked_today = 0
            failed_count = 0
            processed_count = 0

            print(f"\nTotal stocks to process: {len(stocks_to_process)}")
            print(f"Stocks already checked today (no new data): {initial_uptodate_count}")
            print(f"API calls remaining: {self.api_counter.get_remaining(api_threshold)}")
            print()

            # Reset stop event
            self._stop_event.clear()

            if use_threading and self.num_workers > 1:
                # Multi-threaded download with background writer
                # Start background writer thread
                self._bg_writer = BackgroundWriter(self.writer)
                self._bg_writer.start()

                try:
                    with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
                        # Submit all tasks
                        future_to_symbol = {
                            executor.submit(
                                self._process_single_stock,
                                symbol,
                                start_date,
                                end_date,
                                api_threshold,
                            ): symbol
                            for symbol in stocks_to_process
                        }

                        # Process results as they complete
                        with tqdm(total=len(stocks_to_process), desc="Processing") as pbar:
                            for future in as_completed(future_to_symbol):
                                symbol = future_to_symbol[future]
                                try:
                                    result = future.result()
                                    symbol, result_type, df = result

                                    # Submit data to background writer if we have data
                                    if df is not None and not df.empty:
                                        self._bg_writer.submit(WriteTask(symbol, df, result_type))

                                    # Update progress and get category
                                    category = self._update_progress_from_result(
                                        symbol, result_type
                                    )

                                    # Update statistics
                                    if category == "new":
                                        new_downloads += 1
                                    elif category == "incremental":
                                        incremental_updates += 1
                                    elif category == "uptodate":
                                        skipped_uptodate += 1
                                    elif category == "skipped":
                                        skipped_checked_today += 1
                                    elif category == "failed":
                                        failed_count += 1
                                    elif category == "stopped":
                                        # API limit reached, cancel remaining futures
                                        executor.shutdown(wait=False, cancel_futures=True)
                                        print(f"\n\nAPI limit reached ({api_threshold}). Saving progress...")
                                        break

                                except Exception as e:
                                    logger.error(f"Error processing {symbol}: {e}")
                                    failed_count += 1

                                processed_count += 1
                                pbar.update(1)

                                # Save progress periodically
                                if processed_count % BATCH_SIZE == 0:
                                    self._save_progress()

                finally:
                    # Wait for all writes to complete and stop background writer
                    if self._bg_writer:
                        print("\nWaiting for writes to complete...")
                        self._bg_writer.wait_for_completion()
                        self._bg_writer.stop()
                        print(f"Background writer finished: {self._bg_writer.write_count} writes, {self._bg_writer.error_count} errors")

            else:
                # Single-threaded download (original logic)
                for i, symbol in enumerate(tqdm(stocks_to_process, desc="Processing")):
                    # Check API limit
                    if not self.api_counter.check_limit(api_threshold):
                        print(f"\n\nAPI limit reached ({api_threshold}). Saving progress...")
                        break

                    # Skip stocks already checked today with no new data
                    if symbol in self.progress.get("uptodate_stocks", []):
                        skipped_checked_today += 1
                        continue

                    # Check stock's actual max date in H5 file
                    stock_max_date = self.writer.get_5m_stock_max_date(symbol)

                    if stock_max_date:
                        if stock_max_date >= end_date:
                            # Stock is already up to date
                            skipped_uptodate += 1
                            if symbol not in self.progress["completed_stocks"]:
                                self.progress["completed_stocks"].append(symbol)
                            continue
                        else:
                            # Need incremental download
                            download_start = (
                                datetime.strptime(stock_max_date, "%Y-%m-%d")
                                + pd.Timedelta(days=1)
                            ).strftime("%Y-%m-%d")

                            result_code, df = self.download_stock_5m_data(symbol, download_start, end_date)
                            if result_code == "success":
                                # Write directly in single-threaded mode
                                self.writer.write_5m_kline_data(symbol, df, mode="a")
                                incremental_updates += 1
                                if symbol not in self.progress["completed_stocks"]:
                                    self.progress["completed_stocks"].append(symbol)
                                if symbol in self.progress["failed_stocks"]:
                                    self.progress["failed_stocks"].remove(symbol)
                            elif result_code == "no_data":
                                # No new data available - mark as checked today
                                if symbol not in self.progress["uptodate_stocks"]:
                                    self.progress["uptodate_stocks"].append(symbol)
                                if symbol not in self.progress["completed_stocks"]:
                                    self.progress["completed_stocks"].append(symbol)
                            else:  # failed
                                failed_count += 1
                                if symbol not in self.progress["failed_stocks"]:
                                    self.progress["failed_stocks"].append(symbol)
                    else:
                        # Stock not in H5, need full download
                        result_code, df = self.download_stock_5m_data(symbol, start_date, end_date)
                        if result_code == "success":
                            # Write directly in single-threaded mode
                            self.writer.write_5m_kline_data(symbol, df, mode="a")
                            new_downloads += 1
                            if symbol not in self.progress["completed_stocks"]:
                                self.progress["completed_stocks"].append(symbol)
                            if symbol in self.progress["failed_stocks"]:
                                self.progress["failed_stocks"].remove(symbol)
                        elif result_code == "no_data":
                            # No data available for this stock - mark as checked today
                            if symbol not in self.progress["uptodate_stocks"]:
                                self.progress["uptodate_stocks"].append(symbol)
                        else:  # failed
                            failed_count += 1
                            if symbol not in self.progress["failed_stocks"]:
                                self.progress["failed_stocks"].append(symbol)

                    # Save progress periodically
                    if (i + 1) % BATCH_SIZE == 0:
                        self._save_progress()

            # Update completed_end_date if all stocks are now up to date
            total_processed = new_downloads + incremental_updates + skipped_uptodate
            if total_processed == len(stocks_to_process) and failed_count == 0:
                self.progress["completed_end_date"] = end_date
                logger.info(f"Updated completed_end_date to {end_date}")

            # Final progress save
            self._save_progress()

            # Summary
            print("\n" + "=" * 70)
            print("Download Complete!")
            print("=" * 70)
            print(f"This session:")
            print(f"  New stocks downloaded: {new_downloads}")
            print(f"  Incremental updates: {incremental_updates}")
            print(f"  Skipped (up to date in H5): {skipped_uptodate}")
            print(f"  Skipped (checked today, no new data): {skipped_checked_today}")
            print(f"  Failed: {failed_count}")
            print(f"Total progress: {len(self.progress['completed_stocks'])}/{self.progress['total_stocks']}")
            print(f"Failed stocks: {len(self.progress['failed_stocks'])}")
            print(f"Stocks checked today (no new data): {len(self.progress.get('uptodate_stocks', []))}")
            print(f"Data range completed: {self.progress.get('completed_end_date', 'N/A')}")
            print(f"API calls used: {self.api_counter.calls_today}")

            # File size
            if self.writer.ptrade_data_5m_path.exists():
                size_mb = self.writer.ptrade_data_5m_path.stat().st_size / (1024 * 1024)
                print(f"\nOutput file: {self.writer.ptrade_data_5m_path}")
                print(f"File size: {size_mb:.1f} MB")

            return {
                "new_downloads": new_downloads,
                "incremental_updates": incremental_updates,
                "skipped_uptodate": skipped_uptodate,
                "skipped_checked_today": skipped_checked_today,
                "failed": failed_count,
                "total_completed": len(self.progress["completed_stocks"]),
                "total_stocks": self.progress["total_stocks"],
            }

        finally:
            self.fetcher.logout()

    def check_outdated_stocks(self, target_date: str) -> list:
        """
        Check which stocks in H5 file have data not updated to target date

        Args:
            target_date: Target date (YYYY-MM-DD), stocks with max_date < target_date are outdated

        Returns:
            List of (symbol, max_date) tuples for outdated stocks
        """
        print("=" * 70)
        print(f"Checking stocks with data before {target_date}")
        print("=" * 70)

        existing_stocks = self.writer.get_existing_5m_stocks()
        if not existing_stocks:
            print("No stocks found in H5 file")
            return []

        print(f"Total stocks in H5 file: {len(existing_stocks)}")

        outdated_stocks = []
        for symbol in tqdm(existing_stocks, desc="Checking stocks"):
            max_date = self.writer.get_5m_stock_max_date(symbol)
            if max_date and max_date < target_date:
                outdated_stocks.append((symbol, max_date))

        # Sort by max_date
        outdated_stocks.sort(key=lambda x: x[1])

        print(f"\nOutdated stocks (data before {target_date}): {len(outdated_stocks)}")
        return outdated_stocks

    def check_and_fill(
        self,
        target_date: str = DEFAULT_TARGET_DATE,
        max_stocks: int = None,
        max_api_calls: int = None,
    ) -> dict:
        """
        Check which stocks are outdated and fill them with new data

        Args:
            target_date: Target date to update to (YYYY-MM-DD)
            max_stocks: Maximum stocks to process this session
            max_api_calls: Maximum API calls this session

        Returns:
            Summary dict with statistics
        """
        # Check outdated stocks first
        outdated_stocks = self.check_outdated_stocks(target_date)

        if not outdated_stocks:
            print("\nAll stocks are up to date!")
            return {"outdated": 0, "updated": 0, "failed": 0}

        # Print outdated stocks list
        print("\n" + "=" * 70)
        print("Outdated stocks list:")
        print("=" * 70)
        for symbol, max_date in outdated_stocks[:50]:  # Show first 50
            print(f"  {symbol}: last data {max_date}")
        if len(outdated_stocks) > 50:
            print(f"  ... and {len(outdated_stocks) - 50} more")

        # Apply max_stocks limit
        stocks_to_process = [s[0] for s in outdated_stocks]
        if max_stocks and len(stocks_to_process) > max_stocks:
            stocks_to_process = stocks_to_process[:max_stocks]
            print(f"\nLimited to {max_stocks} stocks this session")

        # Set API call limit
        api_threshold = max_api_calls or SAFE_API_THRESHOLD

        print("\n" + "=" * 70)
        print("Starting to fill outdated stocks")
        print("=" * 70)
        print(f"Stocks to process: {len(stocks_to_process)}")
        print(f"Target date: {target_date}")
        print(f"API call limit: {api_threshold}")
        print("=" * 70)

        # Login to BaoStock
        self.fetcher.login()

        try:
            # Download statistics
            updated_count = 0
            failed_count = 0
            no_data_count = 0

            # Single-threaded download (workers fixed to 1)
            for symbol in tqdm(stocks_to_process, desc="Filling data"):
                # Check API limit
                if not self.api_counter.check_limit(api_threshold):
                    print(f"\n\nAPI limit reached ({api_threshold}). Stopping...")
                    break

                # Get stock's current max date
                stock_max_date = self.writer.get_5m_stock_max_date(symbol)
                if not stock_max_date:
                    # Shouldn't happen, but handle it
                    failed_count += 1
                    continue

                # Calculate start date for download
                download_start = (
                    datetime.strptime(stock_max_date, "%Y-%m-%d")
                    + pd.Timedelta(days=1)
                ).strftime("%Y-%m-%d")

                # Download data
                result_code, df = self.download_stock_5m_data(
                    symbol, download_start, target_date
                )

                if result_code == "success":
                    self.writer.write_5m_kline_data(symbol, df, mode="a")
                    updated_count += 1
                    logger.info(f"Updated {symbol}: {len(df)} rows")
                elif result_code == "no_data":
                    no_data_count += 1
                    logger.info(f"No new data for {symbol}")
                else:
                    failed_count += 1
                    logger.error(f"Failed to update {symbol}")

            # Summary
            print("\n" + "=" * 70)
            print("Check and Fill Complete!")
            print("=" * 70)
            print(f"Total outdated stocks: {len(outdated_stocks)}")
            print(f"Processed this session: {len(stocks_to_process)}")
            print(f"Successfully updated: {updated_count}")
            print(f"No new data available: {no_data_count}")
            print(f"Failed: {failed_count}")
            print(f"API calls used: {self.api_counter.calls_today}")

            return {
                "outdated": len(outdated_stocks),
                "processed": len(stocks_to_process),
                "updated": updated_count,
                "no_data": no_data_count,
                "failed": failed_count,
            }

        finally:
            self.fetcher.logout()

    def show_status(self) -> None:
        """Display current download progress"""
        print("=" * 70)
        print("5-Minute K-line Download Status")
        print("=" * 70)

        progress = self._load_progress()

        print(f"Progress file: {self.progress_file}")
        print(f"Last update: {progress.get('last_update', 'N/A')}")
        print(f"Target date range: {progress.get('start_date')} ~ {progress.get('end_date')}")
        print(f"Completed end date: {progress.get('completed_end_date', 'N/A')}")
        print()
        print(f"Total stocks: {progress.get('total_stocks', 0)}")
        print(f"Completed: {len(progress.get('completed_stocks', []))}")
        print(f"Failed: {len(progress.get('failed_stocks', []))}")
        print(f"Remaining: {progress.get('total_stocks', 0) - len(progress.get('completed_stocks', []))}")
        print()
        uptodate_check_date = progress.get("uptodate_check_date", "N/A")
        uptodate_count = len(progress.get("uptodate_stocks", []))
        print(f"Uptodate check date: {uptodate_check_date}")
        print(f"Stocks checked (no new data): {uptodate_count}")
        print()

        # API counter status
        api_date = progress.get("api_calls_date")
        api_calls = progress.get("api_calls_today", 0)
        print(f"API calls ({api_date or 'N/A'}): {api_calls}")

        # File status
        if self.writer.ptrade_data_5m_path.exists():
            size_mb = self.writer.ptrade_data_5m_path.stat().st_size / (1024 * 1024)
            stocks_in_file = len(self.writer.get_existing_5m_stocks())
            print()
            print(f"Output file: {self.writer.ptrade_data_5m_path}")
            print(f"File size: {size_mb:.1f} MB")
            print(f"Stocks in file: {stocks_in_file}")

        print("=" * 70)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Download 5-minute K-line data from BaoStock",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  # Normal download (single-threaded due to server rate limiting)
  python download_5m_kline.py

  # Continue from last progress
  python download_5m_kline.py --resume

  # Limit stocks per session
  python download_5m_kline.py --max-stocks 1000

  # Retry failed stocks
  python download_5m_kline.py --retry-failed

  # Check progress
  python download_5m_kline.py --status

  # Check and fill outdated stocks (default target: {DEFAULT_TARGET_DATE})
  python download_5m_kline.py --check-and-fill

  # Check and fill with custom target date
  python download_5m_kline.py --check-and-fill --target-date 2025-03-15

  # Auto-loop until all stocks are updated (with default 60s interval)
  python download_5m_kline.py --check-and-fill --loop

  # Auto-loop with custom interval (120 seconds between iterations)
  python download_5m_kline.py --check-and-fill --loop --loop-interval 120

  # Only check outdated stocks without filling
  python download_5m_kline.py --check-only
        """,
    )

    parser.add_argument(
        "--resume",
        action="store_true",
        default=True,
        help="Resume from previous progress (default: True)",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start fresh, ignore previous progress",
    )
    parser.add_argument(
        "--max-stocks",
        type=int,
        metavar="N",
        help="Maximum stocks to download this session",
    )
    parser.add_argument(
        "--max-api-calls",
        type=int,
        metavar="N",
        help=f"Maximum API calls this session (default: {SAFE_API_THRESHOLD})",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Only retry previously failed stocks",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        metavar="DATE",
        help=f"Start date (YYYY-MM-DD, default: {KLINE_5M_START_DATE})",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        metavar="DATE",
        help="End date (YYYY-MM-DD, default: today)",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show download progress and exit",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        metavar="N",
        help=f"Number of worker threads (default: {DEFAULT_WORKERS}, max: {MAX_WORKERS})",
    )
    parser.add_argument(
        "--no-threading",
        action="store_true",
        help="Disable multi-threading, use single-threaded mode (deprecated: workers fixed to 1)",
    )
    parser.add_argument(
        "--check-and-fill",
        action="store_true",
        help=f"Check outdated stocks in H5 file and fill them to target date (default: {DEFAULT_TARGET_DATE})",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Only check outdated stocks without filling (use with --target-date)",
    )
    parser.add_argument(
        "--target-date",
        type=str,
        metavar="DATE",
        default=DEFAULT_TARGET_DATE,
        help=f"Target date for --check-and-fill or --check-only (YYYY-MM-DD, default: {DEFAULT_TARGET_DATE})",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Enable auto-loop mode: automatically retry until all stocks are updated",
    )
    parser.add_argument(
        "--loop-interval",
        type=int,
        metavar="SECONDS",
        default=DEFAULT_LOOP_INTERVAL,
        help=f"Interval between loop iterations in seconds (default: {DEFAULT_LOOP_INTERVAL})",
    )

    args = parser.parse_args()

    # Workers fixed to 1 due to server rate limiting (ignore user input)
    num_workers = 1

    # Create downloader with fixed single worker
    downloader = FiveMinuteKlineDownloader(num_workers=num_workers)

    if args.status:
        downloader.show_status()
        return

    # Handle check-only mode
    if args.check_only:
        downloader.check_outdated_stocks(args.target_date)
        return

    # Handle check-and-fill mode
    if args.check_and_fill:
        if args.loop:
            # Loop mode: keep running until all stocks are updated
            iteration = 0
            while True:
                iteration += 1
                print(f"\n{'='*70}")
                print(f"Loop iteration #{iteration}")
                print(f"{'='*70}")

                result = downloader.check_and_fill(
                    target_date=args.target_date,
                    max_stocks=args.max_stocks,
                    max_api_calls=args.max_api_calls,
                )

                # Check if all stocks are up to date
                if result["outdated"] == 0:
                    print(f"\n{'='*70}")
                    print("All stocks are up to date! Loop completed.")
                    print(f"Total iterations: {iteration}")
                    print(f"{'='*70}")
                    break

                # Check if we made progress this iteration
                if result["updated"] == 0 and result["failed"] == 0:
                    print(f"\nNo progress made this iteration (no updates, no failures).")
                    print("This might indicate no new data available or API issues.")
                    print(f"Waiting {args.loop_interval} seconds before retry...")

                # Wait before next iteration
                print(f"\nWaiting {args.loop_interval} seconds before next iteration...")
                print("(Press Ctrl+C to stop)")
                try:
                    time.sleep(args.loop_interval)
                except KeyboardInterrupt:
                    print(f"\n\nLoop interrupted by user after {iteration} iterations.")
                    break

                # Recreate downloader to reset API counter for new day
                downloader = FiveMinuteKlineDownloader(num_workers=num_workers)
        else:
            # Single run mode
            downloader.check_and_fill(
                target_date=args.target_date,
                max_stocks=args.max_stocks,
                max_api_calls=args.max_api_calls,
            )
        return

    # Determine resume mode
    resume = args.resume and not args.no_resume

    # Threading is always disabled (workers fixed to 1)
    use_threading = False

    # Run download
    downloader.download_all(
        max_stocks=args.max_stocks,
        max_api_calls=args.max_api_calls,
        resume=resume,
        retry_failed=args.retry_failed,
        start_date=args.start_date,
        end_date=args.end_date,
        use_threading=use_threading,
    )


if __name__ == "__main__":
    main()
