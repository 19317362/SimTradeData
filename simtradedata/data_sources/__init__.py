"""
数据源模块

提供多个数据源的统一接口，包括mootdx、BaoStock、QStock等。
"""

from .baostock_adapter import BaoStockAdapter
from .base import BaseDataSource
from .manager import DataSourceManager
from .mootdx_adapter import MootdxAdapter
from .qstock_adapter import QStockAdapter

__all__ = [
    "BaseDataSource",
    "DataSourceManager",
    "MootdxAdapter",
    "BaoStockAdapter",
    "QStockAdapter",
]
