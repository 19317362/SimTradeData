"""
FINVALUE 财务字段映射

将 mootdx Affair 返回的财务数据字段映射到数据库标准字段
"""

# FINVALUE 字段ID到标准字段的映射
# 参考: docs/mootdx_api/fields.md

FINVALUE_FIELD_MAPPING = {
    # 报告期
    0: "report_period",  # YYMMDD格式
    # 每股指标
    1: "eps",  # 基本每股收益
    2: "eps_diluted",  # 扣非每股收益
    3: "undistributed_profit_per_share",  # 每股未分配利润
    4: "bps",  # 每股净资产
    5: "capital_reserve_per_share",  # 每股资本公积
    6: "roe",  # 净资产收益率
    7: "operating_cash_flow_per_share",  # 每股经营现金流
    # 资产负债表 - 资产部分
    8: "cash_and_equivalents",  # 货币资金
    11: "accounts_receivable",  # 应收账款
    17: "inventory",  # 存货
    21: "current_assets",  # 流动资产合计
    27: "fixed_assets",  # 固定资产
    33: "intangible_assets",  # 无形资产
    39: "non_current_assets",  # 非流动资产合计
    40: "total_assets",  # 资产总计
    # 资产负债表 - 负债部分
    41: "short_term_borrowing",  # 短期借款
    44: "accounts_payable",  # 应付账款
    54: "current_liabilities",  # 流动负债合计
    55: "long_term_borrowing",  # 长期借款
    62: "non_current_liabilities",  # 非流动负债合计
    63: "total_liabilities",  # 负债合计
    # 资产负债表 - 权益部分
    64: "share_capital",  # 实收资本（股本）
    65: "capital_reserve",  # 资本公积
    66: "surplus_reserve",  # 盈余公积
    68: "retained_earnings",  # 未分配利润
    69: "minority_interests",  # 少数股东权益
    72: "shareholders_equity",  # 所有者权益合计
    # 利润表
    74: "revenue",  # 营业收入
    75: "operating_cost",  # 营业成本
    76: "business_tax_and_surcharge",  # 营业税金及附加
    77: "selling_expenses",  # 销售费用
    78: "administrative_expenses",  # 管理费用
    80: "financial_expenses",  # 财务费用
    81: "asset_impairment_loss",  # 资产减值损失
    86: "operating_profit",  # 营业利润
    90: "total_profit",  # 利润总额
    92: "income_tax",  # 所得税
    93: "net_profit",  # 净利润
    95: "net_profit_after_minority_interests",  # 归属母公司净利润
    # 利润表 - 其他
    96: "eps_basic",  # 基本每股收益（元）
    97: "eps_diluted_profit",  # 稀释每股收益（元）
    # 现金流量表
    139: "cash_received_from_sales",  # 销售商品收到的现金
    157: "operating_cash_inflow",  # 经营活动现金流入小计
    172: "operating_cash_outflow",  # 经营活动现金流出小计
    173: "operating_cash_flow",  # 经营活动现金流量净额
    197: "investing_cash_flow",  # 投资活动现金流量净额
    213: "financing_cash_flow",  # 筹资活动现金流量净额
    222: "cash_increase",  # 现金及现金等价物净增加额
    # 财务比率（计算得出）
    # 这些字段通常不在原始数据中，需要计算
}

# 反向映射：标准字段到FINVALUE ID
STANDARD_TO_FINVALUE = {v: k for k, v in FINVALUE_FIELD_MAPPING.items()}

# mootdx Affair 返回的中文字段名映射
CHINESE_FIELD_MAPPING = {
    # 基础字段
    "code": "code",  # 股票代码
    "market": "market",  # 市场代码
    "report_date": "report_period",  # 报告期
    # 每股指标
    "基本每股收益": "eps",  # 基本每股收益
    "扣除非经常性损益每股收益": "eps_diluted",  # 扣非每股收益
    "每股未分配利润": "undistributed_profit_per_share",  # 每股未分配利润
    "每股净资产": "bps",  # 每股净资产
    "每股资本公积金": "capital_reserve_per_share",  # 每股资本公积
    "净资产收益率": "roe",  # 净资产收益率
    "每股经营现金流量": "operating_cash_flow_per_share",  # 每股经营现金流
    # 资产负债表 - 资产
    "货币资金": "cash_and_equivalents",  # 货币资金
    "应收账款": "accounts_receivable",  # 应收账款
    "存货": "inventory",  # 存货
    "流动资产合计": "current_assets",  # 流动资产合计
    "固定资产": "fixed_assets",  # 固定资产
    "无形资产": "intangible_assets",  # 无形资产
    "非流动资产合计": "non_current_assets",  # 非流动资产合计
    "资产总计": "total_assets",  # 资产总计
    # 资产负债表 - 负债
    "短期借款": "short_term_borrowing",  # 短期借款
    "应付账款": "accounts_payable",  # 应付账款
    "流动负债合计": "current_liabilities",  # 流动负债合计
    "长期借款": "long_term_borrowing",  # 长期借款
    "非流动负债合计": "non_current_liabilities",  # 非流动负债合计
    "负债合计": "total_liabilities",  # 负债合计
    # 资产负债表 - 权益
    "实收资本（或股本）": "share_capital",  # 实收资本（股本）
    "资本公积": "capital_reserve",  # 资本公积
    "盈余公积": "surplus_reserve",  # 盈余公积
    "未分配利润": "retained_earnings",  # 未分配利润
    "少数股东权益": "minority_interests",  # 少数股东权益
    "所有者权益（或股东权益）合计": "shareholders_equity",  # 所有者权益合计
    # 利润表
    "其中：营业收入": "revenue",  # 营业收入
    "其中：营业成本": "operating_cost",  # 营业成本
    "营业税金及附加": "business_tax_and_surcharge",  # 营业税金及附加
    "销售费用": "selling_expenses",  # 销售费用
    "管理费用": "administrative_expenses",  # 管理费用
    "财务费用": "financial_expenses",  # 财务费用
    "资产减值损失": "asset_impairment_loss",  # 资产减值损失
    "三、营业利润": "operating_profit",  # 营业利润
    "四、利润总额": "total_profit",  # 利润总额
    "减：所得税": "income_tax",  # 所得税
    "五、净利润": "net_profit",  # 净利润
    "归属于母公司所有者的净利润": "net_profit_after_minority_interests",  # 归属母公司净利润
    # 现金流量表
    "销售商品、提供劳务收到的现金": "cash_received_from_sales",  # 销售商品收到的现金
    "经营活动现金流入小计": "operating_cash_inflow",  # 经营活动现金流入小计
    "经营活动现金流出小计": "operating_cash_outflow",  # 经营活动现金流出小计
    "经营活动产生的现金流量净额": "operating_cash_flow",  # 经营活动现金流量净额
    "投资活动产生的现金流量净额": "investing_cash_flow",  # 投资活动现金流量净额
    "筹资活动产生的现金流量净额": "financing_cash_flow",  # 筹资活动现金流量净额
    "五、现金及现金等价物净增加额": "cash_increase",  # 现金及现金等价物净增加额
    # 拼音字段（兼容性）
    "liutongguben": "circulating_shares",  # 流通股本
    "zongguben": "total_shares",  # 总股本
    "zongzichan": "total_assets",  # 总资产
    "liudongzichan": "current_assets",  # 流动资产
    "gudingzichan": "fixed_assets",  # 固定资产
    "wuxingzichan": "intangible_assets",  # 无形资产
    "liudongfuzhai": "current_liabilities",  # 流动负债
    "changqifuzhai": "long_term_liabilities",  # 长期负债
    "jingzichan": "net_assets",  # 净资产
    "zhuyingshouru": "revenue",  # 主营收入
    "zhuyinglirun": "operating_profit",  # 主营利润
    "yingshouzhangkuan": "accounts_receivable",  # 应收账款
    "yingyelirun": "operating_profit",  # 营业利润
    "jingyingxianjinliu": "operating_cash_flow",  # 经营现金流
    "shuihoulirun": "net_profit_after_tax",  # 税后利润
    "jinglirun": "net_profit",  # 净利润
    "weifenpeilirun": "retained_earnings",  # 未分配利润
    "meigujingzichan": "bps",  # 每股净资产
    "province": "province",  # 省份
    "industry": "industry",  # 行业
    "updated_date": "updated_date",  # 更新日期
    "ipo_date": "ipo_date",  # 上市日期
}


def map_financial_data(data_dict: dict) -> dict:
    """
    将 mootdx 财务数据映射为标准格式

    Args:
        data_dict: mootdx Affair.parse() 返回的单行数据（字典）

    Returns:
        标准格式的财务数据
    """
    result = {}

    # 首先映射中文字段名
    for chinese_field, standard_field in CHINESE_FIELD_MAPPING.items():
        if chinese_field in data_dict:
            result[standard_field] = data_dict[chinese_field]

    # 然后映射FINVALUE字段（如果存在）
    for finvalue_id, standard_field in FINVALUE_FIELD_MAPPING.items():
        # FINVALUE字段在DataFrame中可能是列名如 'field_1', 'field_40' 等
        field_name = f"field_{finvalue_id}"
        if field_name in data_dict:
            result[standard_field] = data_dict[field_name]

    return result


def get_required_fields_for_db() -> dict:
    """
    获取数据库所需的财务字段映射

    Returns:
        字段映射字典，key为数据库字段名，value为默认值
    """
    return {
        # 基础信息
        "symbol": None,
        "report_date": None,
        "report_type": "Q4",
        # 每股指标
        "eps": 0.0,  # 每股收益
        "bps": 0.0,  # 每股净资产
        "roe": 0.0,  # 净资产收益率
        # 利润表
        "revenue": 0.0,  # 营业收入
        "operating_profit": 0.0,  # 营业利润
        "net_profit": 0.0,  # 净利润
        "gross_margin": 0.0,  # 毛利率（需计算）
        "net_margin": 0.0,  # 净利率（需计算）
        # 资产负债表
        "total_assets": 0.0,  # 总资产
        "total_liabilities": 0.0,  # 总负债
        "shareholders_equity": 0.0,  # 股东权益
        # 现金流量表
        "operating_cash_flow": 0.0,  # 经营现金流
        "investing_cash_flow": 0.0,  # 投资现金流
        "financing_cash_flow": 0.0,  # 筹资现金流
        # 财务比率
        "debt_ratio": 0.0,  # 资产负债率（需计算）
        "roa": 0.0,  # 总资产回报率（需计算）
        # 元数据
        "source": "mootdx",
    }
