"""
SimTradeData 核心基类模块

提供统一的基类和混入类，确保项目架构的一致性。
"""

# 版本信息
__version__ = "1.0.0"

# 导出主要类
from .base_manager import BaseManager
from .config_mixin import ConfigMixin
from .error_handling import (
    ErrorCode,
    ExternalServiceError,
    ResourceNotFoundError,
    ValidationError,
    unified_error_handler,
)
from .logging_mixin import LoggingMixin, log_method_execution
from .utils import extract_data_safely

__all__ = [
    "BaseManager",
    "ConfigMixin",
    "LoggingMixin",
    "unified_error_handler",
    "log_method_execution",
    "ErrorCode",
    "ValidationError",
    "ResourceNotFoundError",
    "ExternalServiceError",
    "extract_data_safely",
]
