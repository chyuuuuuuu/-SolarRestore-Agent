"""Static competition-demo data for the PV clipping restoration agent.

The numbers in this file are intentionally deterministic so the demo can run
without production StarRocks, MySQL, Kafka, S3, or Feishu credentials.
"""

PROJECT = {
    "name": "光擎 SolarRestore Agent",
    "english_name": "SolarRestore Agent",
    "competition_name": "光擎 SolarRestore Agent：全球光伏削峰还原自进化智能体平台",
    "subsystem": "sigen-efficiency-verification / pv_clipping",
    "entrypoint": "python main_test_2.py -> model_dispatch.main_schedule()",
    "schedule": "AI regions run at 19:45 local time",
    "kafka_topic": "PV_RESULT_TOPIC",
    "sample_date": "2026-05-28",
}

MODELS = {
    "baseline": {
        "name": "PowerRestorationModel",
        "s3_key": "sigen-pv-prediction/pv_clipping/best_model_choose8_weighloss_noise_0.pth",
        "stage": "production baseline",
    },
    "candidate": {
        "name": "PowerRestorationModel_10000station",
        "s3_key": "sigen-pv-prediction/pv_clipping/best_model_0.94_10.pth",
        "stage": "gray release",
    },
}

AGENTS = [
    {
        "name": "编排 Agent",
        "status": "已上线",
        "responsibility": "按区域、本地时区和调度策略触发任务",
    },
    {
        "name": "数据接入 Agent",
        "status": "已上线",
        "responsibility": "接入 StarRocks、MySQL、NWP，并对齐 288 个 5 分钟点",
    },
    {
        "name": "检测/场景 Agent",
        "status": "已上线",
        "responsibility": "初筛、明细复筛，生成 clipping_mask 与 scene_vector",
    },
    {
        "name": "还原 Agent",
        "status": "灰度中",
        "responsibility": "新旧 LSTM-Transformer 模型复用同一份输入并行推理",
    },
    {
        "name": "下发 Agent",
        "status": "已上线",
        "responsibility": "通过 Kafka PV_RESULT_TOPIC 下发还原结果",
    },
    {
        "name": "分析 Agent",
        "status": "已上线，持续增强",
        "responsibility": "汇总成功、失败、错误归因、削峰比例和场景分布",
    },
    {
        "name": "进化 Agent",
        "status": "已上线，训练发布持续增强",
        "responsibility": "回流高价值样本，重构数据集与场景标签，驱动候选模型灰度迭代",
    },
]

EUROPE_SUMMARY = {
    "date": PROJECT["sample_date"],
    "covered_regions": 13,
    "total_stations": 29456,
    "potential_clipping_stations": 17142,
    "success_stations": 6079,
    "failed_or_skipped_stations": 11063,
}

REGIONS = [
    {
        "area": "Europe/Berlin",
        "total_stations": 19599,
        "potential_clipping": 9066,
        "success": 2966,
        "failed_or_skipped": 6100,
    },
    {
        "area": "Europe/London",
        "total_stations": 6876,
        "potential_clipping": 5526,
        "success": 1926,
        "failed_or_skipped": 3600,
    },
    {
        "area": "Europe/Madrid",
        "total_stations": 1811,
        "potential_clipping": 1628,
        "success": 908,
        "failed_or_skipped": 720,
    },
    {
        "area": "Europe/Sarajevo",
        "total_stations": 886,
        "potential_clipping": 771,
        "success": 188,
        "failed_or_skipped": 583,
    },
    {
        "area": "Europe/Helsinki",
        "total_stations": 178,
        "potential_clipping": 88,
        "success": 48,
        "failed_or_skipped": 40,
    },
]

ISSUES = [
    {
        "type": "功率数据插值失败",
        "count": 1147,
        "meaning": "数据接入质量问题，需要进入数据质量场景",
        "bucket": "data_quality",
        "action": "沉淀 station/date/power_source，进入功率插值质量集",
    },
    {
        "type": "重复索引导致插值失败",
        "count": 1011,
        "meaning": "构建重复 timestamp / 重复 index 训练前处理规则",
        "bucket": "duplicate_index",
        "action": "训练前统一去重或聚合，接入层强制 index 唯一化",
    },
    {
        "type": "col 未定义历史 bug 风险",
        "count": 136,
        "meaning": "工程链路需修复，避免污染训练样本",
        "bucket": "code_contract",
        "action": "作为工程契约问题拦截，不进入训练集",
    },
    {
        "type": "无功率数据",
        "count": 728,
        "meaning": "标记为数据缺失，不应直接作为负样本训练",
        "bucket": "missing_power",
        "action": "输出区域/站点/日期缺失 TopN，进入数据可用性报表",
    },
    {
        "type": "Empty data passed with indices specified",
        "count": 138,
        "meaning": "气象空表需提前拦截并打标签",
        "bucket": "missing_weather",
        "action": "记录 station/date/weather_source，补充天气缺失兜底策略",
    },
    {
        "type": "clipping mask 计算失败",
        "count": 51,
        "meaning": "新增非 288 点、短日曲线、边界点削峰场景",
        "bucket": "mask_boundary",
        "action": "mask 长度强校验，越界保护，构建短日曲线样本",
    },
    {
        "type": "too many values to unpack",
        "count": 51,
        "meaning": "统一 mask 与 scene_vector 接口契约",
        "bucket": "interface_contract",
        "action": "固定 calculate_clipping_mask 返回结构，并加契约测试",
    },
]

EVOLUTION_STEPS = [
    {"name": "线上每日还原结果", "state": "running"},
    {"name": "日终分析 Agent 归因", "state": "running"},
    {"name": "失败原因与新削峰场景识别", "state": "running"},
    {"name": "高价值样本回流", "state": "running"},
    {"name": "数据集与场景标签重构", "state": "running"},
    {"name": "训练 PowerRestorationModel_10000station", "state": "planned_partial"},
    {"name": "S3 发布候选模型", "state": "planned_partial"},
    {"name": "生产双模型灰度并行", "state": "gray"},
    {"name": "指标评估与主模型晋升", "state": "planned_partial"},
]

SUGGESTED_QUESTIONS = [
    "线上9030 2026-05-22 predict_type=4 多少个站？",
    "欧洲K8s data-platform sigen-pv-clipping日志有什么问题？",
    "今天欧洲削峰还原状态如何？",
    "失败最多的原因是什么？",
    "Europe/Berlin 为什么风险最高？",
    "新旧模型灰度机制怎么评估？",
    "高价值样本回流和数据集重构实现了吗？",
    "比赛答辩时项目亮点怎么讲？",
]
