"""
ETF数据管理器

负责ETF基础信息、成分股数据和净值数据的管理。
"""

# 标准库导入
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

# 项目内导入
from ..config import Config
from ..core import BaseManager, ValidationError, unified_error_handler
from ..database import DatabaseManager

logger = logging.getLogger(__name__)


class ETFDataManager(BaseManager):
    """ETF数据管理器"""

    def __init__(self, db_manager: DatabaseManager, config: Config = None, **kwargs):
        """
        初始化ETF数据管理器

        Args:
            db_manager: 数据库管理器
            config: 配置对象
        """
        super().__init__(config=config, db_manager=db_manager, **kwargs)

    def _init_specific_config(self):
        """初始化ETF特定配置"""
        self.etf_cache_ttl = self._get_config("cache_ttl", 3600)
        self.max_holdings_count = self._get_config("max_holdings_count", 1000)

    def _init_components(self):
        """初始化组件"""
        # ETF类型映射
        self.etf_types = {
            "stock": "股票型ETF",
            "bond": "债券型ETF",
            "commodity": "商品型ETF",
            "currency": "货币型ETF",
            "sector": "行业ETF",
            "index": "指数ETF",
            "inverse": "反向ETF",
            "leveraged": "杠杆ETF",
        }

        # ETF状态
        self.etf_status = {
            "active": "正常交易",
            "suspended": "暂停交易",
            "delisted": "已退市",
            "pending": "待上市",
        }

    def _get_required_attributes(self) -> List[str]:
        """必需属性列表"""
        return ["db_manager", "etf_types", "etf_status"]

    @unified_error_handler(return_dict=True)
    def save_etf_info(self, etf_data: Dict[str, Any]) -> bool:
        """
        保存ETF基础信息

        Args:
            etf_data: ETF基础信息数据

        Returns:
            bool: 保存是否成功
        """
        if not etf_data or not etf_data.get("symbol"):
            raise ValidationError("ETF数据或代码不能为空")

        try:
            # 标准化ETF数据
            standardized_data = self._standardize_etf_info(etf_data)

            sql = """
            INSERT OR REPLACE INTO ptrade_etf_info 
            (symbol, name, name_en, market, exchange, currency, etf_type, 
             underlying_index, management_company, custodian_bank, 
             inception_date, expense_ratio, tracking_error, aum, 
             nav_frequency, dividend_frequency, status, last_update)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            params = (
                standardized_data["symbol"],
                standardized_data["name"],
                standardized_data.get("name_en"),
                standardized_data["market"],
                standardized_data.get("exchange"),
                standardized_data.get("currency", "CNY"),
                standardized_data.get("etf_type", "stock"),
                standardized_data.get("underlying_index"),
                standardized_data.get("management_company"),
                standardized_data.get("custodian_bank"),
                standardized_data.get("inception_date"),
                standardized_data.get("expense_ratio"),
                standardized_data.get("tracking_error"),
                standardized_data.get("aum"),
                standardized_data.get("nav_frequency", "daily"),
                standardized_data.get("dividend_frequency", "quarterly"),
                standardized_data.get("status", "active"),
                datetime.now().isoformat(),
            )

            self.db_manager.execute(sql, params)
            self._log_performance(
                "save_etf_info", 0.1, symbol=standardized_data["symbol"]
            )
            return True

        except Exception as e:
            self._log_error("save_etf_info", e, symbol=etf_data.get("symbol"))
            raise

    @unified_error_handler(return_dict=True)
    def save_etf_holdings(
        self,
        etf_symbol: str,
        holdings_data: List[Dict[str, Any]],
        holding_date: date = None,
    ) -> bool:
        """
        保存ETF成分股数据

        Args:
            etf_symbol: ETF代码
            holdings_data: 成分股数据列表
            holding_date: 持仓日期

        Returns:
            bool: 保存是否成功
        """
        if not etf_symbol:
            raise ValidationError("ETF代码不能为空")

        if not holdings_data:
            raise ValidationError("持仓数据不能为空")

        if len(holdings_data) > self.max_holdings_count:
            raise ValidationError(f"持仓数据过多，最大支持{self.max_holdings_count}条")

        try:
            if holding_date is None:
                holding_date = datetime.now().date()

            # 删除旧的成分股数据
            delete_sql = """
            DELETE FROM ptrade_etf_holdings 
            WHERE etf_symbol = ? AND holding_date = ?
            """
            self.db_manager.execute(delete_sql, (etf_symbol, str(holding_date)))

            # 插入新的成分股数据
            insert_sql = """
            INSERT INTO ptrade_etf_holdings 
            (etf_symbol, holding_date, stock_symbol, stock_name, weight, 
             shares, market_value, sector, last_update)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            saved_count = 0
            current_time = datetime.now().isoformat()

            for holding in holdings_data:
                try:
                    standardized_holding = self._standardize_holding_data(holding)

                    params = (
                        etf_symbol,
                        str(holding_date),
                        standardized_holding["stock_symbol"],
                        standardized_holding.get("stock_name"),
                        standardized_holding.get("weight"),
                        standardized_holding.get("shares"),
                        standardized_holding.get("market_value"),
                        standardized_holding.get("sector"),
                        current_time,
                    )

                    self.db_manager.execute(insert_sql, params)
                    saved_count += 1

                except Exception as e:
                    self._log_error("save_etf_holdings", e, holding=holding)

            self._log_performance(
                "save_etf_holdings", 0.2, symbol=etf_symbol, holdings_count=saved_count
            )
            return saved_count > 0

        except Exception as e:
            self._log_error(
                "save_etf_holdings", e, symbol=etf_symbol, count=len(holdings_data)
            )
            raise

    @unified_error_handler(return_dict=True)
    def save_etf_nav(self, nav_data: Dict[str, Any]) -> bool:
        """
        保存ETF净值数据

        Args:
            nav_data: ETF净值数据

        Returns:
            bool: 保存是否成功
        """
        if not nav_data or not nav_data.get("symbol"):
            raise ValidationError("净值数据或ETF代码不能为空")

        try:
            standardized_data = self._standardize_nav_data(nav_data)

            sql = """
            INSERT OR REPLACE INTO ptrade_etf_nav 
            (symbol, nav_date, unit_nav, accumulated_nav, estimated_nav, 
             premium_discount, creation_redemption, dividend_per_unit, 
             last_update)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            params = (
                standardized_data["symbol"],
                standardized_data["nav_date"],
                standardized_data.get("unit_nav"),
                standardized_data.get("accumulated_nav"),
                standardized_data.get("estimated_nav"),
                standardized_data.get("premium_discount"),
                standardized_data.get("creation_redemption"),
                standardized_data.get("dividend_per_unit"),
                datetime.now().isoformat(),
            )

            self.db_manager.execute(sql, params)

            self._log_performance(
                "save_etf_nav",
                0.05,
                symbol=standardized_data["symbol"],
                nav_date=standardized_data["nav_date"],
            )
            return True

        except Exception as e:
            self._log_error("save_etf_nav", e, symbol=nav_data.get("symbol"))
            raise

    @unified_error_handler(return_dict=True)
    def get_etf_info(self, etf_symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取ETF基础信息

        Args:
            etf_symbol: ETF代码

        Returns:
            Optional[Dict[str, Any]]: ETF基础信息
        """
        if not etf_symbol:
            raise ValidationError("ETF代码不能为空")

        try:
            sql = """
            SELECT * FROM ptrade_etf_info 
            WHERE symbol = ?
            """

            result = self.db_manager.fetchone(sql, (etf_symbol,))

            if result:
                return dict(result)
            else:
                return None

        except Exception as e:
            self._log_error("get_etf_info", e, symbol=etf_symbol)
            raise

    @unified_error_handler(return_dict=True)
    def get_etf_holdings(
        self, etf_symbol: str, holding_date: date = None
    ) -> List[Dict[str, Any]]:
        """
        获取ETF成分股

        Args:
            etf_symbol: ETF代码
            holding_date: 持仓日期，默认为最新

        Returns:
            List[Dict[str, Any]]: 成分股列表
        """
        if not etf_symbol:
            raise ValidationError("ETF代码不能为空")

        try:
            if holding_date is None:
                # 获取最新持仓日期
                date_sql = """
                SELECT MAX(holding_date) as latest_date 
                FROM ptrade_etf_holdings 
                WHERE etf_symbol = ?
                """
                date_result = self.db_manager.fetchone(date_sql, (etf_symbol,))

                if not date_result or not date_result["latest_date"]:
                    return []

                holding_date = datetime.strptime(
                    date_result["latest_date"], "%Y-%m-%d"
                ).date()

            sql = """
            SELECT * FROM ptrade_etf_holdings 
            WHERE etf_symbol = ? AND holding_date = ?
            ORDER BY weight DESC
            """

            results = self.db_manager.fetchall(sql, (etf_symbol, str(holding_date)))

            return [dict(row) for row in results]

        except Exception as e:
            self._log_error("get_etf_holdings", e, symbol=etf_symbol)
            raise

    @unified_error_handler(return_dict=True)
    def get_etf_nav_history(
        self, etf_symbol: str, start_date: date = None, end_date: date = None
    ) -> List[Dict[str, Any]]:
        """
        获取ETF净值历史

        Args:
            etf_symbol: ETF代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            List[Dict[str, Any]]: 净值历史数据
        """
        if not etf_symbol:
            raise ValidationError("ETF代码不能为空")

        try:
            if end_date is None:
                end_date = datetime.now().date()

            if start_date is None:
                start_date = end_date - timedelta(days=30)

            sql = """
            SELECT * FROM ptrade_etf_nav 
            WHERE symbol = ? AND nav_date >= ? AND nav_date <= ?
            ORDER BY nav_date DESC
            """

            results = self.db_manager.fetchall(
                sql, (etf_symbol, str(start_date), str(end_date))
            )

            return [dict(row) for row in results]

        except Exception as e:
            self._log_error("get_etf_nav_history", e, symbol=etf_symbol)
            raise

    @unified_error_handler(return_dict=True)
    def get_etf_list(
        self, etf_type: str = None, market: str = None
    ) -> List[Dict[str, Any]]:
        """
        获取ETF列表

        Args:
            etf_type: ETF类型筛选
            market: 市场筛选

        Returns:
            List[Dict[str, Any]]: ETF列表
        """
        try:
            conditions = []
            params = []

            if etf_type:
                conditions.append("etf_type = ?")
                params.append(etf_type)

            if market:
                conditions.append("market = ?")
                params.append(market)

            where_clause = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)

            sql = f"""
            SELECT symbol, name, etf_type, market, aum, expense_ratio, status
            FROM ptrade_etf_info 
            {where_clause}
            ORDER BY aum DESC
            """

            results = self.db_manager.fetchall(sql, params)

            return [dict(row) for row in results]

        except Exception as e:
            self._log_error("get_etf_list", e, etf_type=etf_type, market=market)
            raise

    @unified_error_handler(return_dict=False)
    def is_etf_exists(self, symbol: str) -> bool:
        """
        检查ETF是否存在

        Args:
            symbol: ETF代码

        Returns:
            bool: 是否存在
        """
        if not symbol:
            return False

        try:
            sql = "SELECT 1 FROM ptrade_etf_info WHERE symbol = ? LIMIT 1"
            result = self.db_manager.fetchone(sql, (symbol,))
            return result is not None

        except Exception as e:
            self._log_error("is_etf_exists", e, symbol=symbol)
            return False

    @unified_error_handler(return_dict=True)
    def get_etf_statistics(self) -> Dict[str, Any]:
        """
        获取ETF统计信息

        Returns:
            Dict[str, Any]: 统计信息
        """
        try:
            # ETF数量统计
            etf_count_sql = "SELECT COUNT(*) as total FROM ptrade_etf_info"
            etf_count = self.db_manager.fetchone(etf_count_sql)

            # 按类型统计
            type_stats_sql = """
            SELECT etf_type, COUNT(*) as count 
            FROM ptrade_etf_info 
            GROUP BY etf_type
            """
            type_stats = self.db_manager.fetchall(type_stats_sql)

            # 按市场统计
            market_stats_sql = """
            SELECT market, COUNT(*) as count 
            FROM ptrade_etf_info 
            GROUP BY market
            """
            market_stats = self.db_manager.fetchall(market_stats_sql)

            return {
                "total_etfs": etf_count["total"] if etf_count else 0,
                "etf_types": self.etf_types,
                "type_distribution": {
                    row["etf_type"]: row["count"] for row in type_stats
                },
                "market_distribution": {
                    row["market"]: row["count"] for row in market_stats
                },
                "supported_features": [
                    "ETF基础信息管理",
                    "ETF成分股跟踪",
                    "ETF净值历史",
                    "ETF业绩分析",
                ],
            }

        except Exception as e:
            self._log_error("get_etf_statistics", e)
            raise

    def _standardize_etf_info(self, etf_data: Dict[str, Any]) -> Dict[str, Any]:
        """标准化ETF基础信息"""
        standardized = {
            "symbol": etf_data.get("symbol", "").upper(),
            "name": etf_data.get("name", ""),
            "market": etf_data.get("market", "SZ"),
        }

        # 复制其他字段
        for key, value in etf_data.items():
            if key not in standardized:
                standardized[key] = value

        return standardized

    def _standardize_holding_data(self, holding_data: Dict[str, Any]) -> Dict[str, Any]:
        """标准化成分股数据"""
        standardized = {
            "stock_symbol": holding_data.get("stock_symbol", "").upper(),
            "weight": self._parse_float(holding_data.get("weight")),
            "shares": self._parse_float(holding_data.get("shares")),
            "market_value": self._parse_float(holding_data.get("market_value")),
        }

        # 复制其他字段
        for key, value in holding_data.items():
            if key not in standardized:
                standardized[key] = value

        return standardized

    def _standardize_nav_data(self, nav_data: Dict[str, Any]) -> Dict[str, Any]:
        """标准化净值数据"""
        standardized = {
            "symbol": nav_data.get("symbol", "").upper(),
            "nav_date": nav_data.get("nav_date", ""),
            "unit_nav": self._parse_float(nav_data.get("unit_nav")),
            "accumulated_nav": self._parse_float(nav_data.get("accumulated_nav")),
            "estimated_nav": self._parse_float(nav_data.get("estimated_nav")),
            "premium_discount": self._parse_float(nav_data.get("premium_discount")),
        }

        # 复制其他字段
        for key, value in nav_data.items():
            if key not in standardized:
                standardized[key] = value

        return standardized

    def _parse_float(self, value: Any) -> Optional[float]:
        """解析浮点数"""
        if value is None or value == "":
            return None

        try:
            return float(value)
        except (ValueError, TypeError):
            return None
