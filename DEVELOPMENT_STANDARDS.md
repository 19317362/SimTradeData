# SimTradeData 开发规范

## 1. 类设计规范

### 1.1 Manager类设计
- **必须继承** `BaseManager` 基类
- **构造函数参数顺序**：必需依赖 -> 可选依赖 -> config -> **kwargs
- **配置参数获取**：统一使用 `_get_config()` 方法
- **错误处理**：使用 `@unified_error_handler` 装饰器

```python
from simtradedata.core import BaseManager, unified_error_handler

class ExampleManager(BaseManager):
    def __init__(self, 
                 db_manager: DatabaseManager,  # 必需依赖
                 cache_manager: CacheManager = None,  # 可选依赖
                 config: Config = None,  # 配置对象
                 **kwargs):
        super().__init__(config=config, 
                        db_manager=db_manager, 
                        cache_manager=cache_manager,
                        **kwargs)
    
    def _init_specific_config(self):
        self.custom_param = self._get_config("custom_param", "default")
    
    def _init_components(self):
        # 初始化子组件
        pass
    
    def _get_required_attributes(self) -> List[str]:
        return ["db_manager"]
    
    @unified_error_handler(return_dict=True)
    def public_method(self, param: str):
        # 公共方法实现
        pass
```

### 1.2 接口设计规范

#### 方法命名规范
- **查询方法**：`get_` + 数据类型（如 `get_stock_data`）
- **保存方法**：`save_` + 数据类型（如 `save_market_data`）
- **删除方法**：`delete_` + 数据类型（如 `delete_stock_data`）
- **布尔方法**：`is_` + 状态（如 `is_valid`）或 `has_` + 资源（如 `has_data`）

#### 参数顺序规范
1. **主要标识符**（如 symbol）
2. **时间参数**（如 date_range）
3. **配置选项**（如 options）
4. **可选参数**（使用默认值）

```python
def get_stock_data(self, 
                  symbol: str,              # 主要标识符
                  date_range: DateRange,    # 时间参数
                  options: QueryOptions = None) -> DataResult:  # 配置选项
    pass
```

## 2. 错误处理规范

### 2.1 异常类型
- **ValidationError**：参数验证失败
- **ResourceNotFoundError**：资源未找到
- **ExternalServiceError**：外部服务错误
- **DatabaseError**：数据库错误

### 2.2 错误处理装饰器
```python
@unified_error_handler(return_dict=True, log_errors=True)
def public_method(self, param: str) -> DataResult:
    # 方法实现
    pass
```

## 3. 日志记录规范

### 3.1 日志级别使用
- **DEBUG**：详细的调试信息，仅开发环境
- **INFO**：正常操作信息，关键步骤
- **WARNING**：警告信息，可恢复的错误
- **ERROR**：错误信息，影响功能但不崩溃
- **CRITICAL**：严重错误，系统崩溃

### 3.2 日志格式规范
```python
# 方法开始
self.logger.info(f"开始{method_name}")

# 方法完成
self.logger.info(f"{method_name}执行完成: 处理{count}条记录, 耗时{duration:.2f}s")

# 错误日志
self.logger.error(f"[{method_name}] 执行失败: {error}, 上下文: {context}")
```

## 4. 配置管理规范

### 4.1 配置键命名规范
- **格式**：`模块名.功能.参数名`
- **示例**：`database.connection.timeout`、`sync.gap_detection.max_days`

### 4.2 默认值规范
- **性能参数**：超时30s，重试3次，批量100条
- **缓存参数**：TTL 1小时，最大1000条
- **日志参数**：INFO级别，性能日志开启

## 5. 导入语句规范

### 5.1 导入顺序
```python
# 1. 标准库 - 按字母顺序
import logging
import sqlite3
from datetime import date, datetime

# 2. 第三方库 - 按字母顺序  
import pandas as pd
import yaml

# 3. 项目内导入 - 按依赖层级
from ..config import Config
from ..core.base_manager import BaseManager
from .utils import helper_function
```

### 5.2 导入规范
- **避免** `from module import *`
- **使用** 相对导入引用项目内模块
- **明确** 导入具体类和函数，避免整个模块导入

## 6. 测试规范

### 6.1 测试文件组织
- **单元测试**：`tests/unit/test_module_name.py`
- **集成测试**：`tests/integration/test_workflow_name.py`
- **端到端测试**：`tests/e2e/test_feature_name.py`

### 6.2 测试命名规范
```python
class TestManagerName:
    def test_method_name_success_case(self):
        """测试正常情况"""
        pass
    
    def test_method_name_with_invalid_params(self):
        """测试参数验证"""
        pass
    
    def test_method_name_when_resource_not_found(self):
        """测试资源不存在情况"""
        pass
```

## 7. 代码审查检查清单

### 7.1 架构检查
- [ ] 是否继承了合适的基类？
- [ ] 是否使用了统一的错误处理？
- [ ] 是否遵循了接口设计规范？

### 7.2 代码质量检查
- [ ] 是否有重复代码？
- [ ] 是否有适当的日志记录？
- [ ] 是否有单元测试覆盖？
- [ ] 是否通过了类型检查？

### 7.3 性能检查
- [ ] 是否有合理的缓存策略？
- [ ] 是否有资源泄漏风险？
- [ ] 是否有性能瓶颈？

通过遵循这些规范，我们可以确保代码的一致性、可维护性和质量。