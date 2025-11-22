"""
BaoStock data fetcher implementation
"""

import logging
from datetime import datetime

import baostock as bs
import pandas as pd

from simtradedata.utils.code_utils import convert_from_ptrade_code, retry_on_failure

logger = logging.getLogger(__name__)


class BaoStockFetcher:
    """
    Fetch data from BaoStock API

    BaoStock provides free A-share market data including:
    - Daily K-line data
    - Financial statements
    - Valuation indicators
    - Adjust factors
    - Dividend data
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
                logger.info("BaoStock logout successful")

    def __enter__(self):
        self.login()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logout()
        return False  # Don't suppress exceptions

    def __del__(self):
        """Destructor to ensure logout on object deletion"""
        try:
            self.logout()
        except:
            pass  # Ignore errors in destructor


    @retry_on_failure
    def fetch_adjust_factor(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        Fetch adjust factors

        Args:
            symbol: Stock code in PTrade format
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            DataFrame with columns: date, foreAdjustFactor, backAdjustFactor
        """

        bs_code = convert_from_ptrade_code(symbol, "baostock")

        rs = bs.query_adjust_factor(
            code=bs_code, start_date=start_date, end_date=end_date
        )

        if rs.error_code != "0":
            raise RuntimeError(
                f"Failed to query adjust factor for {symbol}: {rs.error_msg}"
            )

        df = rs.get_data()

        if df.empty:
            # Check if it's an index (indices don't have adjust factors)
            if bs_code.startswith("sh.") and bs_code[3:].startswith("00"):
                logger.debug(f"No adjust factor data for index {symbol} (expected)")
            elif bs_code.startswith("sz.399"):  # Shenzhen indices
                logger.debug(f"No adjust factor data for index {symbol} (expected)")
            else:
                logger.warning(f"No adjust factor data for {symbol}")
            return pd.DataFrame()

        # Note: BaoStock returns 'dividOperateDate', not 'date'
        df = df.rename(columns={"dividOperateDate": "date"})

        df["date"] = pd.to_datetime(df["date"])
        df["foreAdjustFactor"] = pd.to_numeric(df["foreAdjustFactor"], errors="coerce")
        df["backAdjustFactor"] = pd.to_numeric(df["backAdjustFactor"], errors="coerce")

        # Note: Keep 'date' as column for converter to handle

        logger.info(f"Fetched {len(df)} adjust factor rows for {symbol}")

        return df


    @retry_on_failure
    def fetch_stock_basic(self, symbol: str) -> pd.DataFrame:
        """
        Fetch stock basic information

        Args:
            symbol: Stock code in PTrade format

        Returns:
            DataFrame with basic stock information
        """

        bs_code = convert_from_ptrade_code(symbol, "baostock")
        rs = bs.query_stock_basic(code=bs_code)

        if rs.error_code != "0":
            raise RuntimeError(
                f"Failed to query stock basic info for {symbol}: {rs.error_msg}"
            )

        df = rs.get_data()

        if df.empty:
            return pd.DataFrame()

        return df


    @retry_on_failure
    def fetch_stock_industry(self, symbol: str, date: str = None) -> pd.DataFrame:
        """
        Fetch stock industry classification

        Args:
            symbol: Stock code in PTrade format
            date: Date string (YYYY-MM-DD), if None use today

        Returns:
            DataFrame with industry classification
        """

        bs_code = convert_from_ptrade_code(symbol, "baostock")

        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        # Normalize date format
        date_str = date.replace("-", "")

        rs = bs.query_stock_industry(code=bs_code, date=date_str)

        if rs.error_code != "0":
            raise RuntimeError(f"Failed to query industry for {symbol}: {rs.error_msg}")

        df = rs.get_data()

        if df.empty:
            logger.warning(f"No industry data for {symbol}")
            return pd.DataFrame()

        return df

    @retry_on_failure
    def fetch_trade_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch trading calendar

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            DataFrame with trading days
        """

        rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)

        if rs.error_code != "0":
            raise RuntimeError(f"Failed to query trade calendar: {rs.error_msg}")

        df = rs.get_data()

        if df.empty:
            return pd.DataFrame()

        return df

    @retry_on_failure
    def fetch_index_stocks(self, index_code: str, date: str = None) -> pd.DataFrame:
        """
        Fetch index constituent stocks

        Args:
            index_code: Index code in PTrade format (e.g., '000016.SS', '000300.SS', '000905.SS')
            date: Date string (YYYY-MM-DD), if None use latest

        Returns:
            DataFrame with stock codes

        Note:
            BaoStock only supports specific indices:
            - 000016.SS (上证50): query_sz50_stocks
            - 000300.SS (沪深300): query_hs300_stocks
            - 000905.SS (中证500): query_zz500_stocks
        """

        query_date = date
        if query_date is None:
            query_date = datetime.now().strftime("%Y-%m-%d")

        # Map PTrade index codes to BaoStock query methods
        index_map = {
            "000016.SS": "sz50",  # 上证50
            "000300.SS": "hs300",  # 沪深300
            "000905.SS": "zz500",  # 中证500
        }

        if index_code not in index_map:
            logger.warning(f"Index {index_code} not supported by BaoStock")
            return pd.DataFrame()

        index_type = index_map[index_code]

        # Call corresponding BaoStock API
        if index_type == "sz50":
            rs = bs.query_sz50_stocks(date=query_date)
        elif index_type == "hs300":
            rs = bs.query_hs300_stocks(date=query_date)
        elif index_type == "zz500":
            rs = bs.query_zz500_stocks(date=query_date)
        else:
            logger.warning(f"Unknown index type: {index_type}")
            return pd.DataFrame()

        if rs.error_code != "0":
            raise RuntimeError(
                f"Failed to query index stocks for {index_code}: {rs.error_msg}"
            )

        df = rs.get_data()

        if df.empty:
            logger.warning(f"No constituent stocks found for {index_code}")
            return pd.DataFrame()

        return df
