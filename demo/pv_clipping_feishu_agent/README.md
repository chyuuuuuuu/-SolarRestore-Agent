# 光擎 SolarRestore Agent Demo

光擎 SolarRestore Agent 是“全球光伏削峰还原自进化智能体平台”的比赛初稿 demo，把 `pv_clipping` 的生产闭环抽象成可运行的“监控 + 提问 + 飞书推送”服务。默认带静态样例兜底，也支持只读接入线上 StarRocks 9030 查询 `sigen_device.pv_prediction_history`，并可通过 K8s Dashboard 拉取 `data-platform` 容器日志做实时归因。

## 运行

```bash
cd SolarRestore-Agent
pip install -r requirements.txt
python3 demo/pv_clipping_feishu_agent/server.py --host 0.0.0.0 --port 8876
```

打开：

```text
http://127.0.0.1:8876
```

## 接入线上数据

### 1. StarRocks 9030 削峰结果

看板“线上 StarRocks 9030 数据”支持任意选择日期和站点，统计 `predict_type=4` 的削峰还原入库结果：

```bash
curl 'http://127.0.0.1:8876/api/online/clipping-summary?date=2026-05-22&station_id=2026021600036'
```

核心口径：

```sql
SELECT COUNT(DISTINCT station_id)
FROM sigen_device.pv_prediction_history
WHERE dt = ?
  AND predict_type = 4;
```

### 2. 欧洲 K8s 容器日志

配置 K8s Dashboard 入口和只读 token：

```bash
export PV_CLIPPING_K8S_DASHBOARD_URL="https://your-k8s-dashboard.example.com"
export PV_CLIPPING_K8S_TOKEN="不要写入代码或提交到仓库"
```

看板“欧洲 K8s 实时日志”默认查询：

```text
namespace = data-platform
keywords  = sigen-pv-clipping
```

也可直接调用：

```bash
curl 'http://127.0.0.1:8876/api/k8s/log-summary?namespace=data-platform&keywords=sigen-pv-clipping&limit_pods=2&tail_lines=500'
```

日志分析会自动统计 ERROR/WARNING、功率插值失败、重复索引、无功率数据、气象空表、mask 计算失败、接口契约异常等问题类型。

## 接入飞书

### 1. 群机器人告警

配置飞书自定义机器人 webhook：

```bash
export FEISHU_WEBHOOK_URL="https://open.feishu.cn/open-apis/bot/v2/hook/xxx"
export FEISHU_WEBHOOK_SECRET="可选签名密钥"
```

页面点击“发送飞书告警”，会推送一张日终监控卡片。未配置 webhook 时会进入 dry-run，只展示 payload。

### 2. 交互确认卡片

页面点击“发送确认卡片”，Agent 会生成一张 Yes/No 卡片：

```text
Agent 发起问题 -> 用户点击 Yes/No -> 飞书回调 -> Agent 收到 yes/no -> 更新原卡片状态
```

未配置飞书凭证时，demo 会 dry-run 展示 payload；配置应用凭证后可发送到指定会话：

```bash
export FEISHU_APP_ID="cli_xxx"
export FEISHU_APP_SECRET="xxx"
```

如果公司封装了自己的飞书卡片 patch 网关，可覆盖更新地址：

```bash
export FEISHU_CARD_PATCH_URL="https://your-feishu-gateway/card/patch"
```

### 3. 飞书应用事件回调

将飞书应用的事件请求地址配置为：

```text
http://<你的服务地址>:8876/feishu/events
```

如需让机器人在会话中自动回复，配置：

```bash
export FEISHU_APP_ID="cli_xxx"
export FEISHU_APP_SECRET="xxx"
```

支持的问题示例：

- 今天欧洲削峰还原状态如何？
- 失败最多的原因是什么？
- Europe/Berlin 为什么风险最高？
- 新旧模型灰度机制怎么评估？
- 高价值样本回流和数据集重构实现了吗？
- 比赛答辩时项目亮点怎么讲？

## API

```bash
curl http://127.0.0.1:8876/api/status
curl -X POST http://127.0.0.1:8876/api/ask \
  -H 'Content-Type: application/json' \
  -d '{"question":"失败最多的原因是什么？"}'
curl -X POST http://127.0.0.1:8876/api/simulate-run \
  -H 'Content-Type: application/json' \
  -d '{"area":"Europe/*"}'
curl -X POST http://127.0.0.1:8876/api/cards/confirm \
  -H 'Content-Type: application/json' \
  -d '{"question":"候选模型是否进入灰度评估？"}'
curl 'http://127.0.0.1:8876/api/online/clipping-summary?date=2026-05-22&station_id=2026021600036'
curl 'http://127.0.0.1:8876/api/k8s/log-summary?namespace=data-platform&keywords=sigen-pv-clipping'
```

实时事件通道：

```text
ws://127.0.0.1:8876/ws/events
```

## Demo 边界

- 当前内置的是 2026-05-28 欧洲线上日志样例，用于比赛稳定演示；线上 9030 查询用于读取实际入库结果。
- 线上削峰站点数口径：`SELECT COUNT(DISTINCT station_id) FROM sigen_device.pv_prediction_history WHERE dt = ? AND predict_type = 4`。
- K8s 日志接入使用 Dashboard 只读接口：先交换 `jweToken`，再读取 `api/v1/log/source/...` 和 `api/v1/log/...`。如果返回 `MSG_LOGIN_UNAUTHORIZED_ERROR`，需要更换有效 token 或检查 RBAC 日志权限。
- 站点明细默认只返回 `dt/station_id/predict_type/model_name/model_version/record_time`；需要完整 `select *` 时追加 `include_payload=1`。
- 生产化时，可继续把 `agent_core.build_monitor_snapshot()` 的静态区域统计替换为日志解析、DB 查询或已有日终分析产物。
- 高价值样本回流、数据集与场景标签重构已在 demo 中实现；自动训练、S3 发布和模型晋升策略仍属于规划/部分实现，本 demo 会如实展示这个边界。
