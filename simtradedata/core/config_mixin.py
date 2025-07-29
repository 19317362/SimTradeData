"""
配置处理混入类

提供统一的配置参数初始化和验证功能。
"""

# 标准库导入
from abc import abstractmethod
from typing import Any, Dict


class ConfigMixin:
    """配置处理混入类"""

    def _init_config_params(self):
        """统一配置参数初始化流程"""
        # 基础配置
        self._init_base_config()

        # 性能配置
        self._init_performance_config()

        # 日志配置
        self._init_logging_config()

        # 子类特定配置
        self._init_specific_config()

        # 验证配置
        self._validate_config()

    def _init_base_config(self):
        """初始化基础配置"""
        self.timeout = self._get_config("timeout", 30)
        self.max_retries = self._get_config("max_retries", 3)
        self.retry_delay = self._get_config("retry_delay", 1.0)
        self.enable_cache = self._get_config("enable_cache", True)

    def _init_performance_config(self):
        """初始化性能相关配置"""
        self.batch_size = self._get_config("batch_size", 100)
        self.connection_pool_size = self._get_config("connection_pool_size", 10)
        self.query_timeout = self._get_config("query_timeout", 30)
        self.cache_ttl = self._get_config("cache_ttl", 3600)

    def _init_logging_config(self):
        """初始化日志相关配置"""
        self.enable_debug = self._get_config("enable_debug", False)
        self.enable_performance_log = self._get_config("enable_performance_log", True)
        self.log_level = self._get_config("log_level", "INFO")

    @abstractmethod
    def _init_specific_config(self):
        """子类实现特定配置初始化"""

    def _validate_config(self):
        """验证配置参数的有效性"""
        if self.timeout <= 0:
            raise ValueError(f"timeout必须大于0: {self.timeout}")
        if self.max_retries < 0:
            raise ValueError(f"max_retries不能小于0: {self.max_retries}")
        if self.batch_size <= 0:
            raise ValueError(f"batch_size必须大于0: {self.batch_size}")

    def get_config_summary(self) -> Dict[str, Any]:
        """获取配置摘要"""
        return {
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "batch_size": self.batch_size,
            "enable_cache": self.enable_cache,
            "enable_debug": self.enable_debug,
        }
