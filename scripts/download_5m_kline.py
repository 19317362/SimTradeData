# -*- coding: utf-8 -*-
"""
5-minute K-line data download program

This program downloads 5-minute K-line data from BaoStock API with:
1. Progress tracking via JSON file (supports resume)
2. API call limit control (default: 100,000 calls/day)
3. Independent storage in ptrade_data_5m.h5
4. Multiple run support to complete full download

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
import warnings
from datetime import datetime, date

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
    """API call counter with daily limit tracking"""

    def __init__(self, daily_limit: int = DAILY_API_LIMIT):
        self.daily_limit = daily_limit
        self.calls_today = 0
        self.calls_date = None

    def check_limit(self, threshold: int = None) -> bool:
        """
        Check if API calls are below limit

        Args:
            threshold: Custom threshold (default: SAFE_API_THRESHOLD)

        Returns:
            True if below limit, False otherwise
        """
        threshold = threshold or SAFE_API_THRESHOLD
        today = date.today()

        # Reset counter if new day
        if self.calls_date != today:
            self.calls_today = 0
            self.calls_date = today

        return self.calls_today < threshold

    def increment(self, count: int = 1) -> None:
        """Increment API call counter"""
        self.calls_today += count

    def get_remaining(self, threshold: int = None) -> int:
        """Get remaining API calls before threshold"""
        threshold = threshold or SAFE_API_THRESHOLD
        return max(0, threshold - self.calls_today)

    def to_dict(self) -> dict:
        """Export counter state"""
        return {
            "api_calls_today": self.calls_today,
            "api_calls_date": str(self.calls_date) if self.calls_date else None,
        }

    def from_dict(self, data: dict) -> None:
        """Import counter state"""
        self.calls_today = data.get("api_calls_today", 0)
        date_str = data.get("api_calls_date")
        if date_str and date_str != "None":
            try:
                self.calls_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                self.calls_date = None
        else:
            self.calls_date = None


class FiveMinuteKlineDownloader:
    """5-minute K-line data downloader with progress tracking"""

    def __init__(
        self,
        output_dir: str = OUTPUT_DIR,
        progress_file: str = PROGRESS_FILE,
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.progress_file = Path(progress_file)

        # Initialize components
        self.fetcher = UnifiedDataFetcher()
        self.writer = HDF5Writer(output_dir=output_dir)

        # API counter
        self.api_counter = APICounter()

        # Load progress
        self.progress = self._load_progress()

        # Restore API counter state
        self.api_counter.from_dict(self.progress)

        # Track failures in current session
        self.session_failed = []

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
        }

    def _save_progress(self) -> None:
        """Save download progress to JSON file"""
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
    ) -> bool:
        """
        Download 5-minute K-line data for a single stock

        Args:
            symbol: Stock code in PTrade format
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            True if successful, False otherwise
        """
        try:
            # Fetch data
            df = self.fetcher.fetch_5m_kline_data(symbol, start_date, end_date)

            # Increment API counter
            self.api_counter.increment()

            if df.empty:
                logger.warning(f"No 5m data for {symbol}")
                return True  # Consider empty result as "success" (no data available)

            # Write to HDF5
            self.writer.write_5m_kline_data(symbol, df, mode="a")

            logger.info(f"Downloaded 5m data for {symbol}: {len(df)} rows")
            return True

        except Exception as e:
            logger.error(f"Failed to download {symbol}: {e}")
            self.session_failed.append(symbol)
            return False

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

    def download_all(
        self,
        max_stocks: int = None,
        max_api_calls: int = None,
        resume: bool = True,
        retry_failed: bool = False,
        start_date: str = None,
        end_date: str = None,
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

            # Download loop - check each stock's actual max date
            new_downloads = 0
            incremental_updates = 0
            skipped_uptodate = 0
            failed_count = 0

            print(f"\nTotal stocks to process: {len(stocks_to_process)}")
            print(f"API calls remaining: {self.api_counter.get_remaining(api_threshold)}")
            print()

            for i, symbol in enumerate(tqdm(stocks_to_process, desc="Processing")):
                # Check API limit
                if not self.api_counter.check_limit(api_threshold):
                    print(f"\n\nAPI limit reached ({api_threshold}). Saving progress...")
                    break

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

                        if self.download_stock_5m_data(symbol, download_start, end_date):
                            incremental_updates += 1
                            if symbol not in self.progress["completed_stocks"]:
                                self.progress["completed_stocks"].append(symbol)
                            if symbol in self.progress["failed_stocks"]:
                                self.progress["failed_stocks"].remove(symbol)
                        else:
                            failed_count += 1
                            if symbol not in self.progress["failed_stocks"]:
                                self.progress["failed_stocks"].append(symbol)
                else:
                    # Stock not in H5, need full download
                    if self.download_stock_5m_data(symbol, start_date, end_date):
                        new_downloads += 1
                        if symbol not in self.progress["completed_stocks"]:
                            self.progress["completed_stocks"].append(symbol)
                        if symbol in self.progress["failed_stocks"]:
                            self.progress["failed_stocks"].remove(symbol)
                    else:
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
            print(f"  Skipped (up to date): {skipped_uptodate}")
            print(f"  Failed: {failed_count}")
            print(f"Total progress: {len(self.progress['completed_stocks'])}/{self.progress['total_stocks']}")
            print(f"Failed stocks: {len(self.progress['failed_stocks'])}")
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
                "failed": failed_count,
                "total_completed": len(self.progress["completed_stocks"]),
                "total_stocks": self.progress["total_stocks"],
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
        epilog="""
Examples:
  # First run (will save progress automatically)
  python download_5m_kline.py

  # Continue from last progress
  python download_5m_kline.py --resume

  # Limit stocks per session
  python download_5m_kline.py --max-stocks 1000

  # Retry failed stocks
  python download_5m_kline.py --retry-failed

  # Check progress
  python download_5m_kline.py --status
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

    args = parser.parse_args()

    # Create downloader
    downloader = FiveMinuteKlineDownloader()

    if args.status:
        downloader.show_status()
        return

    # Determine resume mode
    resume = args.resume and not args.no_resume

    # Run download
    downloader.download_all(
        max_stocks=args.max_stocks,
        max_api_calls=args.max_api_calls,
        resume=resume,
        retry_failed=args.retry_failed,
        start_date=args.start_date,
        end_date=args.end_date,
    )


if __name__ == "__main__":
    main()
