# 大模型 Agent 接入飞书卡片交互说明

这份说明面向新员工，配合当前 demo 理解“Agent 发卡片、用户点击、系统收回调、Agent 继续执行”的最小闭环。

## 演示目标

这次演示要说明的是：大模型 Agent 如何通过飞书进入真实业务交互链路，而不是只发一段文本。

核心闭环：

```text
Agent 发起问题
  -> 飞书展示交互卡片
  -> 用户点击 Yes / No
  -> 飞书开放平台回调
  -> 服务接收事件
  -> WebSocket 实时推给 Agent / 页面
  -> Agent 根据 yes / no 继续执行业务动作
  -> 原卡片被更新为已确认或已拒绝
```

## 当前 demo 已实现的能力

- 发送 PV 削峰还原日终监控卡片。
- 发送 Yes/No 确认卡片。
- 解析飞书卡片按钮回调。
- 把按钮选择转换成 Agent 可理解的 `yes` 或 `no` 信号。
- 用 `event_id`、`task_id`、`user_id` 等信息做防重复点击。
- 用户点击后更新原卡片状态，dry-run 时展示 patch payload。
- 通过 `ws://<host>:<port>/ws/events` 实时推送事件。

## 为什么需要 WebSocket

飞书卡片点击是一个异步事件。WebSocket 的作用是把这个事件实时传给 Agent 或演示页面：

```text
飞书卡片点击事件 -> HTTP 回调 -> 服务端状态机 -> WebSocket -> Agent 上下文
```

当前 demo 的 WebSocket 地址：

```text
ws://127.0.0.1:8876/ws/events
```

## 为什么需要 skill

大模型只负责判断和生成内容，不能天然操作飞书，也不能天然知道用户点了什么。外部动作应拆成清晰 skill：

- `send_feishu_monitor_card`：发送监控卡片。
- `send_feishu_confirmation_card`：需要用户确认时发送 Yes/No 卡片。
- `handle_feishu_card_callback`：解析回调，生成 `yes` / `no` 信号。
- `patch_feishu_card`：用户点击后更新原卡片，移除按钮。
- `summarize_interaction_context`：把长对话压缩成结构化状态。

推荐的上下文状态：

```json
{
  "task": "确认是否进入模型晋升灰度评估",
  "task_id": "task_xxx",
  "card_status": "answered",
  "user_choice": "yes",
  "next_step": "continue_execution"
}
```

## 发送新卡片 vs 更新原卡片

发送新卡片实现简单，但会刷屏：

```text
卡片 1：是否确认？
卡片 2：你选择了 Yes
卡片 3：下一步操作
```

更推荐更新原卡片：

```text
原卡片：是否确认？[Yes] [No]
点击后：已选择 Yes，按钮移除
```

这样用户知道操作已生效，也能避免重复点击。

## 防重复点击

重复点击应保证只处理一次。当前 demo 的策略：

- 优先使用飞书 `event_id` 去重。
- 如果没有 `event_id`，用 `task_id + user_id + action + message_id/card_id` 生成事件键。
- 如果任务已经是 `answered`，后续点击只记录为 duplicate，不再触发业务动作。

## 接入真实飞书时的配置

基础凭证：

```bash
export FEISHU_APP_ID="cli_xxx"
export FEISHU_APP_SECRET="xxx"
```

群机器人 webhook：

```bash
export FEISHU_WEBHOOK_URL="https://open.feishu.cn/open-apis/bot/v2/hook/xxx"
export FEISHU_WEBHOOK_SECRET="可选签名密钥"
```

如果公司通过统一网关封装卡片更新接口：

```bash
export FEISHU_CARD_PATCH_URL="https://your-feishu-gateway/card/patch"
```

事件回调地址：

```text
http://<服务地址>:8876/feishu/events
```

## API 对照

- `POST /api/cards/confirm`：创建并发送确认卡片。
- `POST /api/cards/callback`：本地模拟用户点击 Yes/No。
- `POST /feishu/events`：真实飞书事件回调入口。
- `GET /api/events`：查看最近事件。
- `GET /api/status`：查看监控状态、交互任务和最近事件。
- `GET /ws/events`：WebSocket 实时事件流。

## 后续生产化重点

- 用 Lark CLI 管理应用权限、事件订阅和回调地址。
- 通过 AI Gateway 调用公司统一大模型服务，API Key 不落仓库、不进日志。
- 把当前内存状态机替换为 Redis 或数据库，支持服务重启恢复。
- 卡片更新接口按公司飞书网关或开放平台实际返回字段调整。
- 把回调事件压缩成结构化上下文，避免 Agent 在长上下文里误判旧状态。
- 给每个 skill 写清楚触发条件、输入字段、幂等规则和失败 fallback。
