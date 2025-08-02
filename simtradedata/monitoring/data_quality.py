"""
数据质量监控器

负责监控和报告数据质量问题。
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from ..core.base_manager import BaseManager
from ..database import DatabaseManager

logger = logging.getLogger(__name__)


class DataQualityMonitor(BaseManager):
    """数据质量监控器"""

    def __init__(self, db_manager: DatabaseManager, config=None, **kwargs):
        """
        初始化数据质量监控器

        Args:
            db_manager: 数据库管理器
            config: 配置对象
        """
        super().__init__(config=config, db_manager=db_manager, **kwargs)

    def _init_specific_config(self):
        """初始化特定配置"""
        self.quality_thresholds = {
            "min_price": 0.01,  # 最小股价
            "max_price": 10000,  # 最大股价
            "min_volume": 0,  # 最小成交量
            "max_volume": 1e12,  # 最大成交量
            "min_pe": -1000,  # 最小PE
            "max_pe": 1000,  # 最大PE
            "min_pb": 0,  # 最小PB
            "max_pb": 100,  # 最大PB
        }

    def _init_components(self):
        """初始化组件"""

    def _get_required_attributes(self) -> List[str]:
        """必需属性列表"""
        return ["db_manager"]

    def generate_quality_report(
        self, start_date: Optional[date] = None, end_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        生成数据质量报告

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            Dict[str, Any]: 质量报告
        """
        if not end_date:
            end_date = datetime.now().date()
        if not start_date:
            start_date = end_date - timedelta(days=30)

        report = {
            "report_date": datetime.now().isoformat(),
            "period": {"start": str(start_date), "end": str(end_date)},
            "market_data": self._check_market_data_quality(start_date, end_date),
            "financial_data": self._check_financial_data_quality(),
            "valuation_data": self._check_valuation_data_quality(),
            "stock_info": self._check_stock_info_quality(),
            "summary": {},
        }

        # 计算总体质量评分
        report["summary"] = self._calculate_quality_summary(report)

        return report

    def _check_market_data_quality(
        self, start_date: date, end_date: date
    ) -> Dict[str, Any]:
        """检查市场数据质量"""
        # 总记录数
        total_records = self.db_manager.fetchone(
            "SELECT COUNT(*) as count FROM market_data WHERE date BETWEEN ? AND ?",
            (str(start_date), str(end_date)),
        )["count"]

        # 异常价格数据
        abnormal_prices = self.db_manager.fetchone(
            """
            SELECT COUNT(*) as count FROM market_data 
            WHERE date BETWEEN ? AND ? 
            AND (close <= ? OR close >= ? OR open <= 0 OR high <= 0 OR low <= 0)
            """,
            (
                str(start_date),
                str(end_date),
                self.quality_thresholds["min_price"],
                self.quality_thresholds["max_price"],
            ),
        )["count"]

        # 零成交量数据
        zero_volume = self.db_manager.fetchone(
            "SELECT COUNT(*) as count FROM market_data WHERE date BETWEEN ? AND ? AND volume = 0",
            (str(start_date), str(end_date)),
        )["count"]

        # 价格一致性检查（开高低收的逻辑关系）
        price_logic_errors = self.db_manager.fetchone(
            """
            SELECT COUNT(*) as count FROM market_data 
            WHERE date BETWEEN ? AND ? 
            AND (high < low OR high < open OR high < close OR low > open OR low > close)
            """,
            (str(start_date), str(end_date)),
        )["count"]

        return {
            "total_records": total_records,
            "abnormal_prices": abnormal_prices,
            "zero_volume": zero_volume,
            "price_logic_errors": price_logic_errors,
            "quality_rate": (
                (total_records - abnormal_prices - price_logic_errors) / total_records
                if total_records > 0
                else 0
            )
            * 100,
        }

    def _check_financial_data_quality(self) -> Dict[str, Any]:
        """检查财务数据质量"""
        # 总记录数
        total_records = self.db_manager.fetchone(
            "SELECT COUNT(*) as count FROM financials"
        )["count"]

        # 空收入数据
        null_revenue = self.db_manager.fetchone(
            "SELECT COUNT(*) as count FROM financials WHERE revenue IS NULL OR revenue = 0"
        )["count"]

        # 未来报告期数据
        future_reports = self.db_manager.fetchone(
            "SELECT COUNT(*) as count FROM financials WHERE report_date > date('now')"
        )["count"]

        # 过于陈旧的数据（超过5年）
        old_reports = self.db_manager.fetchone(
            "SELECT COUNT(*) as count FROM financials WHERE report_date < date('now', '-5 years')"
        )["count"]

        return {
            "total_records": total_records,
            "null_revenue": null_revenue,
            "future_reports": future_reports,
            "old_reports": old_reports,
            "quality_rate": (
                (total_records - null_revenue - future_reports) / total_records
                if total_records > 0
                else 0
            )
            * 100,
        }

    def _check_valuation_data_quality(self) -> Dict[str, Any]:
        """检查估值数据质量"""
        # 总记录数
        total_records = self.db_manager.fetchone(
            "SELECT COUNT(*) as count FROM valuations"
        )["count"]

        # 异常PE比
        abnormal_pe = self.db_manager.fetchone(
            """
            SELECT COUNT(*) as count FROM valuations 
            WHERE pe_ratio IS NOT NULL AND (pe_ratio <= ? OR pe_ratio >= ?)
            """,
            (self.quality_thresholds["min_pe"], self.quality_thresholds["max_pe"]),
        )["count"]

        # 异常PB比
        abnormal_pb = self.db_manager.fetchone(
            """
            SELECT COUNT(*) as count FROM valuations 
            WHERE pb_ratio IS NOT NULL AND (pb_ratio <= ? OR pb_ratio >= ?)
            """,
            (self.quality_thresholds["min_pb"], self.quality_thresholds["max_pb"]),
        )["count"]

        # 空数据
        null_data = self.db_manager.fetchone(
            """
            SELECT COUNT(*) as count FROM valuations 
            WHERE (pe_ratio IS NULL OR pe_ratio = 0) 
            AND (pb_ratio IS NULL OR pb_ratio = 0)
            AND (market_cap IS NULL OR market_cap = 0)
            """
        )["count"]

        return {
            "total_records": total_records,
            "abnormal_pe": abnormal_pe,
            "abnormal_pb": abnormal_pb,
            "null_data": null_data,
            "quality_rate": (
                (total_records - abnormal_pe - abnormal_pb - null_data) / total_records
                if total_records > 0
                else 0
            )
            * 100,
        }

    def _check_stock_info_quality(self) -> Dict[str, Any]:
        """检查股票基础信息质量"""
        # 总股票数
        total_stocks = self.db_manager.fetchone("SELECT COUNT(*) as count FROM stocks")[
            "count"
        ]

        # 缺失股本信息
        missing_shares = self.db_manager.fetchone(
            """
            SELECT COUNT(*) as count FROM stocks 
            WHERE total_shares IS NULL OR total_shares = 0
            """
        )["count"]

        # 缺失名称
        missing_names = self.db_manager.fetchone(
            "SELECT COUNT(*) as count FROM stocks WHERE name IS NULL OR name = ''"
        )["count"]

        # 缺失上市日期
        missing_list_date = self.db_manager.fetchone(
            "SELECT COUNT(*) as count FROM stocks WHERE list_date IS NULL"
        )["count"]

        return {
            "total_stocks": total_stocks,
            "missing_shares": missing_shares,
            "missing_names": missing_names,
            "missing_list_date": missing_list_date,
            "quality_rate": (
                (total_stocks - missing_shares - missing_names) / total_stocks
                if total_stocks > 0
                else 0
            )
            * 100,
        }

    def _calculate_quality_summary(self, report: Dict[str, Any]) -> Dict[str, Any]:
        """计算质量汇总"""
        market_quality = report["market_data"]["quality_rate"]
        financial_quality = report["financial_data"]["quality_rate"]
        valuation_quality = report["valuation_data"]["quality_rate"]
        stock_quality = report["stock_info"]["quality_rate"]

        # 计算加权平均质量分数
        weights = {"market": 0.4, "financial": 0.3, "valuation": 0.2, "stock": 0.1}

        overall_quality = (
            market_quality * weights["market"]
            + financial_quality * weights["financial"]
            + valuation_quality * weights["valuation"]
            + stock_quality * weights["stock"]
        )

        # 质量等级
        if overall_quality >= 90:
            quality_grade = "优秀"
        elif overall_quality >= 80:
            quality_grade = "良好"
        elif overall_quality >= 70:
            quality_grade = "一般"
        elif overall_quality >= 60:
            quality_grade = "较差"
        else:
            quality_grade = "很差"

        return {
            "overall_quality_score": round(overall_quality, 2),
            "quality_grade": quality_grade,
            "individual_scores": {
                "market_data": round(market_quality, 2),
                "financial_data": round(financial_quality, 2),
                "valuation_data": round(valuation_quality, 2),
                "stock_info": round(stock_quality, 2),
            },
            "recommendations": self._get_quality_recommendations(report),
        }

    def _get_quality_recommendations(self, report: Dict[str, Any]) -> List[str]:
        """获取质量改进建议"""
        recommendations = []

        # 市场数据建议
        market_data = report["market_data"]
        if market_data["abnormal_prices"] > 0:
            recommendations.append(
                f"发现{market_data['abnormal_prices']}条异常价格数据，建议检查数据源"
            )
        if market_data["zero_volume"] > market_data["total_records"] * 0.01:
            recommendations.append("零成交量数据过多，可能需要检查交易日历或停牌信息")

        # 财务数据建议
        financial_data = report["financial_data"]
        if financial_data["null_revenue"] > 0:
            recommendations.append("存在收入为空的财务数据，建议重新获取或标记为无效")
        if financial_data["future_reports"] > 0:
            recommendations.append("存在未来日期的财务报告，建议修正报告期")

        # 估值数据建议
        valuation_data = report["valuation_data"]
        if valuation_data["null_data"] > valuation_data["total_records"] * 0.5:
            recommendations.append("估值数据缺失过多，建议检查数据源配置")

        # 股票信息建议
        stock_info = report["stock_info"]
        if stock_info["missing_shares"] > stock_info["total_stocks"] * 0.1:
            recommendations.append("股本信息缺失严重，建议重新同步股票基础信息")

        if not recommendations:
            recommendations.append("数据质量良好，继续保持")

        return recommendations

    def alert_quality_issues(self, threshold: float = 80.0) -> Optional[Dict[str, Any]]:
        """
        质量问题告警

        Args:
            threshold: 质量阈值，低于此值时触发告警

        Returns:
            Optional[Dict[str, Any]]: 告警信息，无问题时返回None
        """
        report = self.generate_quality_report()
        overall_score = report["summary"]["overall_quality_score"]

        if overall_score < threshold:
            return {
                "alert_type": "DATA_QUALITY_LOW",
                "severity": "HIGH" if overall_score < 60 else "MEDIUM",
                "message": f"数据质量评分{overall_score:.1f}%，低于阈值{threshold}%",
                "details": report,
                "timestamp": datetime.now().isoformat(),
            }

        return None
