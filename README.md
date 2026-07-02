# 光擎 SolarRestore Agent

光擎 SolarRestore Agent 是一个面向全球光伏削峰还原业务的可运行 demo。它把“监控、提问、日志归因、样本回流、飞书确认卡片”做成一个轻量 Web Agent，用于展示削峰还原生产闭环。

## 在线展示

静态展示页放在 `docs/index.html`，适合通过 GitHub Pages 分享给外部观众。这个页面不依赖后端，展示的是 demo 的核心能力和样例数据。

如果 GitHub Pages 已启用，访问地址通常是：

```text
https://chyuuuuuuu.github.io/-SolarRestore-Agent/
```

## 本地运行完整 Demo

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 demo/pv_clipping_feishu_agent/server.py --host 0.0.0.0 --port 8876
```

打开：

```text
http://127.0.0.1:8876
```

## 可选外部集成

所有外部凭证都通过环境变量注入，不要写入代码或提交到仓库。

```bash
export PV_CLIPPING_STARROCKS_HOST="your-starrocks-host"
export PV_CLIPPING_STARROCKS_PORT="9030"
export PV_CLIPPING_STARROCKS_USER="your-user"
export PV_CLIPPING_STARROCKS_PASSWORD="your-password"

export PV_CLIPPING_K8S_DASHBOARD_URL="https://your-k8s-dashboard.example.com"
export PV_CLIPPING_K8S_TOKEN="your-readonly-token"

export FEISHU_WEBHOOK_URL="https://open.feishu.cn/open-apis/bot/v2/hook/xxx"
export FEISHU_WEBHOOK_SECRET="optional-signing-secret"
export FEISHU_APP_ID="cli_xxx"
export FEISHU_APP_SECRET="your-app-secret"
```

未配置外部系统时，服务会使用内置样例数据，并对飞书推送进入 dry-run。

## API 示例

```bash
curl http://127.0.0.1:8876/api/status
curl -X POST http://127.0.0.1:8876/api/ask \
  -H 'Content-Type: application/json' \
  -d '{"question":"失败最多的原因是什么？"}'
curl -X POST http://127.0.0.1:8876/api/simulate-run \
  -H 'Content-Type: application/json' \
  -d '{"area":"Europe/*"}'
```

## 项目边界

- `docs/` 是公开展示页，适合直接分享。
- `demo/pv_clipping_feishu_agent/` 是可运行的本地 demo 服务。
- 线上数据库、K8s 和飞书能力需要自行配置只读凭证。
- 仓库不包含生产密钥、内部配置文件或模型权重。
