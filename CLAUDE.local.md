# SimTradeData 架构参考

**声明**: 架构设计的唯一权威文档。PTrade API参考: `docs/PTrade_API_mini_Reference.md`

## 分层架构

```
用户接口层 → 业务逻辑层 → 数据同步层 → 性能优化层 → 监控运维层 → 数据存储层
```

## 核心模块

```
simtradedata/
├── interfaces/      # PTrade适配器、REST API、API网关
├── api/            # API路由、查询构建、格式化、缓存
├── markets/        # 多市场管理
├── extended_data/  # 行业数据、ETF、技术指标
├── preprocessor/   # 数据预处理引擎
├── sync/           # 增量更新、验证、缺口检测
├── performance/    # 性能优化
├── monitoring/     # 监控系统
├── database/       # 数据库管理
├── data_sources/   # 数据源管理
├── core/          # 核心功能
├── config/        # 配置管理
└── utils/         # 工具函数
```
