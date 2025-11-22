"""
Unified data fetcher for efficient BaoStock data download

This module provides a unified interface to fetch multiple data types
in a single API call, reducing redundant queries.
"""

import logging
from typing import Optional

import baostock as bs
import pandas as pd

from simtradedata.utils.code_utils import convert_from_ptrade_code, retry_on_failure

logger = logging.getLogger(__name__)


# All fields that can be fetched from query_history_k_data_plus in one call
UNIFIED_DAILY_FIELDS = [
    # === Market data (ptrade_data.h5/stock_data) ===
    "date", "open", "high", "low", "close", "volume", "amount",
    
    # === Valuation data (ptrade_fundamentals.h5/valuation) ===
    "peTTM",      # PE ratio TTM
    "pbMRQ",      # PB ratio
    "psTTM",      # PS ratio TTM
    "pcfNcfTTM",  # PCF ratio TTM
    "turn",       # Turnover rate
    
    # === Status data (for building stock_status_history) ===
    "isST",       # ST status (1=ST, 0=normal)
    "tradestatus" # Trading status (1=normal, 0=halted)
]


class UnifiedDataFetcher:
    """
    Fetch all daily data types in a single API call
    
    This fetcher optimizes BaoStock API usage by fetching market data,
    valuation data, and status data in one query_history_k_data_plus call.
    """
    
    def __init__(self):
        self._logged_in = False
    
    def login(self):
        """Login to BaoStock"""
        if not self._logged_in:
            lg = bs.login()
            if lg.error_code != "0":
                raise ConnectionError(f"BaoStock login failed: {lg.error_msg}")
            self._logged_in = True
            logger.info("BaoStock login successful")
    
    def logout(self):
        """Logout from BaoStock"""
        if self._logged_in:
            try:
                bs.logout()
            except:
                # Ignore all logout errors - connection may already be closed
                pass
            finally:
                self._logged_in = False
    
    def __enter__(self):
        self.login()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logout()
        return False
    
    def __del__(self):
        """Destructor to ensure logout on object deletion"""
        try:
            self.logout()
        except:
            pass
    
    def fetch_unified_daily_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        frequency: str = "d",
        adjustflag: str = "3"
    ) -> pd.DataFrame:
        """
        Fetch all daily data types in a single API call
        
        This method fetches market data, valuation data, and status data
        in one query, significantly reducing API calls.
        
        Args:
            symbol: Stock code in PTrade format (e.g., '600000.SH')
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            frequency: d=daily, w=weekly, m=monthly
            adjustflag: "1"=forward, "2"=backward, "3"=none
        
        Returns:
            DataFrame with all fields: market + valuation + status
            Columns: date, open, high, low, close, volume, amount,
                    peTTM, pbMRQ, psTTM, pcfNcfTTM, turn, isST, tradestatus
        """
        # Convert to BaoStock format
        bs_code = convert_from_ptrade_code(symbol, "baostock")
        
        # Build fields string (all fields in one call)
        fields_str = ",".join(UNIFIED_DAILY_FIELDS)
        
        # Single API call to fetch all data
        rs = bs.query_history_k_data_plus(
            bs_code,
            fields_str,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            adjustflag=adjustflag,
        )
        
        if rs.error_code != "0":
            raise RuntimeError(
                f"Failed to query unified data for {symbol}: {rs.error_msg}"
            )
        
        df = rs.get_data()
        
        if df.empty:
            logger.warning(f"No unified data for {symbol}")
            return pd.DataFrame()
        
        # Convert data types
        df["date"] = pd.to_datetime(df["date"])
        
        # Convert all numeric columns
        numeric_cols = [c for c in df.columns if c != "date"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        logger.info(
            f"Fetched unified data for {symbol}: {len(df)} rows, "
            f"{len(df.columns)} fields"
        )
        
        return df
    
    def fetch_unified_daily_data_batch(
        self,
        symbols: list,
        start_date: str,
        end_date: str,
        frequency: str = "d",
        adjustflag: str = "3"
    ) -> dict:
        """
        Fetch unified daily data for multiple stocks
        
        Args:
            symbols: List of stock codes in PTrade format
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            frequency: d=daily, w=weekly, m=monthly
            adjustflag: "1"=forward, "2"=backward, "3"=none
        
        Returns:
            Dict mapping symbol to DataFrame
        """
        result = {}
        
        for symbol in symbols:
            try:
                df = self.fetch_unified_daily_data(
                    symbol, start_date, end_date, frequency, adjustflag
                )
                if not df.empty:
                    result[symbol] = df
            except Exception as e:
                logger.error(f"Failed to fetch unified data for {symbol}: {e}")
        
        logger.info(
            f"Batch fetch complete: {len(result)}/{len(symbols)} stocks successful"
        )
        
        return result
