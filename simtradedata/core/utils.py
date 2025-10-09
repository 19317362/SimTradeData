"""
核心工具函数

提供项目中通用的工具函数，避免代码重复。
"""

from typing import Any


def extract_data_safely(data: Any) -> Any:
    """
    统一的数据格式处理方法，处理多层嵌套的API响应

    支持的格式：
    1. {"success": True, "data": ...} - 标准成功响应
    2. {"success": False, "error": ...} - 标准失败响应
    3. {"data": ...} - 简单包装格式（没有success字段）
    4. 原始数据 - 直接返回

    Args:
        data: 可能被包装的数据

    Returns:
        Any: 拆包后的实际数据，失败响应返回None
    """
    if not data:
        return None

    # 处理标准失败响应（最优先检查）
    if isinstance(data, dict) and "success" in data and not data.get("success"):
        # 失败响应，返回None
        return None

    # 递归解包多层success/data结构
    # 处理嵌套的标准响应格式
    while (
        isinstance(data, dict)
        and "success" in data
        and data.get("success") is True
        and "data" in data
    ):
        data = data["data"]

    # 处理简单包装格式 {"data": ...}（没有success字段）
    if isinstance(data, dict) and "data" in data and "success" not in data:
        return data["data"]

    # 返回最终数据
    return data
