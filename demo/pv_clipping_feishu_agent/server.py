"""Local HTTP demo for the PV clipping Feishu Agent."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Tuple
from urllib.parse import urlparse

try:
    from .agent_core import (
        FeishuNotifier,
        answer_question,
        build_daily_report,
        build_feishu_card,
        build_monitor_snapshot,
        list_agent_events,
        handle_feishu_event,
        send_confirmation_card,
        simulate_daily_run,
        simulate_card_callback,
    )
    from .online_data import (
        query_online_clipping_summary,
        query_online_restoration_curve,
        query_online_station_history,
    )
    from .k8s_logs import parse_keywords, query_k8s_log_summary
except ImportError:  # Allow `python demo/.../server.py` from repo root.
    import pathlib
    import sys

    current = pathlib.Path(__file__).resolve()
    sys.path.insert(0, str(current.parents[2]))
    from demo.pv_clipping_feishu_agent.agent_core import (  # type: ignore
        FeishuNotifier,
        answer_question,
        build_daily_report,
        build_feishu_card,
        build_monitor_snapshot,
        list_agent_events,
        handle_feishu_event,
        send_confirmation_card,
        simulate_daily_run,
        simulate_card_callback,
    )
    from demo.pv_clipping_feishu_agent.online_data import (  # type: ignore
        query_online_clipping_summary,
        query_online_restoration_curve,
        query_online_station_history,
    )
    from demo.pv_clipping_feishu_agent.k8s_logs import (  # type: ignore
        parse_keywords,
        query_k8s_log_summary,
    )


WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
WS_CLIENTS = set()
WS_LOCK = threading.RLock()


def _ws_frame(payload: Dict[str, Any]) -> bytes:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    length = len(data)
    if length < 126:
        return b"\x81" + bytes([length]) + data
    if length < 65536:
        return b"\x81\x7e" + length.to_bytes(2, "big") + data
    return b"\x81\x7f" + length.to_bytes(8, "big") + data


def _ws_send(sock, payload: Dict[str, Any]) -> bool:
    try:
        sock.sendall(_ws_frame(payload))
        return True
    except OSError:
        return False


def broadcast_ws(kind: str, payload: Dict[str, Any]) -> None:
    message = {"kind": kind, "payload": payload, "sent_at": time.time()}
    with WS_LOCK:
        dead = []
        for sock in WS_CLIENTS:
            if not _ws_send(sock, message):
                dead.append(sock)
        for sock in dead:
            WS_CLIENTS.discard(sock)


INDEX_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>光擎 SolarRestore Agent</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f2f5f8;
      --surface: #ffffff;
      --line: #d9e0e8;
      --text: #1d2733;
      --muted: #647383;
      --blue: #2f6fed;
      --green: #138a55;
      --amber: #b46904;
      --red: #c93636;
      --cyan: #087f8c;
      --ink: #17212f;
      --soft-blue: #eaf1ff;
      --soft-green: #eaf7f0;
      --soft-amber: #fff5df;
      --soft-red: #fff0f0;
      --shadow: 0 10px 28px rgba(21, 34, 50, .08);
    }
    * { box-sizing: border-box; }
    html, body { margin: 0; min-height: 100%; font-family: Inter, "PingFang SC", "Microsoft YaHei", Arial, sans-serif; background: var(--bg); color: var(--text); }
    button, input, textarea { font: inherit; }
    .topbar { position: sticky; top: 0; z-index: 10; border-bottom: 1px solid var(--line); background: rgba(255,255,255,.96); backdrop-filter: blur(10px); }
    .topbar-inner { max-width: 1440px; margin: 0 auto; padding: 14px 24px; display: flex; align-items: center; justify-content: space-between; gap: 18px; }
    .brand { min-width: 280px; }
    .brand h1 { margin: 0; font-size: 21px; line-height: 1.2; font-weight: 800; color: var(--ink); }
    .brand p { margin: 4px 0 0; color: var(--muted); font-size: 13px; }
    .actions { display: flex; gap: 10px; flex-wrap: wrap; justify-content: flex-end; }
    .btn { border: 1px solid var(--line); background: var(--surface); color: var(--text); padding: 9px 12px; border-radius: 6px; cursor: pointer; min-height: 38px; }
    .btn:hover { border-color: #aeb9c7; }
    .btn.primary { background: var(--blue); border-color: var(--blue); color: white; }
    main { max-width: 1440px; margin: 0 auto; padding: 22px 24px 36px; }
    .hero-panel { margin-bottom: 18px; padding: 18px; border: 1px solid #cdd7e3; border-radius: 8px; background: #ffffff; box-shadow: var(--shadow); display: grid; grid-template-columns: minmax(0, 1.18fr) minmax(380px, .82fr); gap: 18px; align-items: stretch; }
    .hero-copy { display: grid; gap: 12px; align-content: center; }
    .eyebrow { color: var(--cyan); font-size: 12px; font-weight: 750; text-transform: uppercase; letter-spacing: 0; }
    .hero-copy h2 { margin: 0; font-size: 32px; line-height: 1.12; letter-spacing: 0; color: var(--ink); }
    .hero-copy p { margin: 0; color: #4a5a6b; font-size: 14px; line-height: 1.55; max-width: 860px; }
    .hero-badges { display: flex; flex-wrap: wrap; gap: 8px; }
    .hero-badge { display: inline-flex; align-items: center; min-height: 28px; padding: 4px 9px; border: 1px solid var(--line); border-radius: 999px; color: #334155; background: #f8fafc; font-size: 12px; }
    .hero-visual { display: grid; grid-template-columns: 1.06fr .94fr; grid-template-rows: minmax(170px, 1fr) auto; gap: 10px; min-height: 260px; }
    .visual-card { border: 1px solid var(--line); border-radius: 8px; background: #fbfcfd; overflow: hidden; position: relative; min-height: 170px; }
    .visual-card svg { width: 100%; height: 100%; min-height: 170px; display: block; }
    .visual-caption { position: absolute; left: 12px; right: 12px; bottom: 10px; padding: 8px 10px; border: 1px solid rgba(217,224,232,.92); border-radius: 8px; background: rgba(255,255,255,.9); backdrop-filter: blur(6px); display: grid; gap: 2px; }
    .visual-caption strong { color: var(--ink); font-size: 13px; }
    .visual-caption span { color: var(--muted); font-size: 12px; }
    .hero-facts { grid-column: 1 / -1; border: 1px solid var(--line); border-radius: 8px; background: #fbfcfd; display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); overflow: hidden; }
    .mini-fact { padding: 10px 12px; border-right: 1px solid var(--line); display: grid; gap: 4px; }
    .mini-fact:last-child { border-right: 0; }
    .mini-fact span { color: var(--muted); font-size: 12px; }
    .mini-fact strong { color: var(--ink); font-size: 13px; line-height: 1.25; }
    .proof-strip { margin-bottom: 18px; display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }
    .proof-item { border: 1px solid var(--line); border-radius: 8px; background: var(--surface); box-shadow: var(--shadow); padding: 13px 14px; display: grid; gap: 7px; min-height: 112px; position: relative; overflow: hidden; }
    .proof-item::after { content: ""; position: absolute; left: 0; right: 0; bottom: 0; height: 3px; background: var(--blue); }
    .proof-item:nth-child(2)::after { background: var(--green); }
    .proof-item:nth-child(3)::after { background: var(--cyan); }
    .proof-item:nth-child(4)::after { background: var(--amber); }
    .proof-top { display: flex; align-items: center; justify-content: space-between; gap: 10px; color: var(--muted); font-size: 12px; }
    .proof-icon { width: 28px; height: 28px; border-radius: 8px; display: grid; place-items: center; background: var(--soft-blue); color: var(--blue); font-weight: 800; }
    .proof-item:nth-child(2) .proof-icon { background: var(--soft-green); color: var(--green); }
    .proof-item:nth-child(3) .proof-icon { background: #eaf8fa; color: var(--cyan); }
    .proof-item:nth-child(4) .proof-icon { background: var(--soft-amber); color: var(--amber); }
    .proof-item strong { color: var(--ink); font-size: 18px; line-height: 1.2; }
    .proof-item p { margin: 0; color: var(--muted); font-size: 12px; line-height: 1.45; }
    .layout { display: grid; grid-template-columns: minmax(0, 1fr) minmax(340px, .36fr); gap: 18px; align-items: start; }
    section { background: var(--surface); border: 1px solid var(--line); border-radius: 8px; box-shadow: var(--shadow); }
    .section-head { padding: 15px 16px 10px; border-bottom: 1px solid var(--line); display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .section-head h2 { margin: 0; font-size: 16px; }
    .section-head span { color: var(--muted); font-size: 12px; }
    .head-actions { display: flex; align-items: center; justify-content: flex-end; gap: 8px; flex-wrap: wrap; }
    .btn.mini { min-height: 30px; padding: 5px 9px; font-size: 12px; }
    .metrics { display: grid; grid-template-columns: repeat(5, minmax(132px, 1fr)); gap: 12px; padding: 16px; }
    .metric { border: 1px solid var(--line); border-radius: 8px; padding: 13px; min-height: 104px; display: flex; flex-direction: column; justify-content: space-between; background: #fbfcfd; position: relative; overflow: hidden; }
    .metric::before { content: ""; position: absolute; left: 0; top: 0; bottom: 0; width: 4px; background: #94a3b8; }
    .metric.ok::before { background: var(--green); }
    .metric.warn::before { background: var(--amber); }
    .metric.danger::before { background: var(--red); }
    .metric label { color: var(--muted); font-size: 12px; }
    .metric strong { font-size: 25px; line-height: 1.15; }
    .metric small { color: var(--muted); font-size: 12px; }
    .metric.ok strong { color: var(--green); }
    .metric.warn strong { color: var(--amber); }
    .metric.danger strong { color: var(--red); }
    .split { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; margin-top: 18px; }
    .viz-split { grid-template-columns: minmax(0, 1.08fr) minmax(0, .92fr); }
    .triple { display: grid; grid-template-columns: minmax(0, 1.1fr) minmax(300px, .9fr); gap: 18px; margin-top: 18px; }
    .system-flow { margin-top: 18px; padding: 16px; display: grid; grid-template-columns: minmax(260px, .32fr) minmax(0, 1fr); gap: 14px; align-items: stretch; }
    .flow-art-panel { border: 1px solid var(--line); border-radius: 8px; background: #fbfcfd; position: relative; overflow: hidden; min-height: 276px; display: grid; }
    .flow-art-panel svg { width: 100%; height: 100%; min-height: 276px; display: block; }
    .flow-art-copy { position: absolute; left: 14px; right: 14px; bottom: 14px; display: grid; gap: 5px; padding: 11px 12px; border: 1px solid rgba(217,224,232,.92); border-radius: 8px; background: rgba(255,255,255,.92); backdrop-filter: blur(6px); }
    .flow-art-copy strong { color: var(--ink); font-size: 14px; }
    .flow-art-copy span { color: var(--muted); font-size: 12px; line-height: 1.4; }
    .flow-track { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; align-content: stretch; }
    .flow-node { border: 1px solid var(--line); border-radius: 8px; background: #fbfcfd; padding: 12px; min-height: 132px; display: grid; align-content: space-between; gap: 9px; position: relative; overflow: hidden; }
    .flow-node::before { content: ""; position: absolute; left: 0; right: 0; top: 0; height: 4px; background: var(--blue); }
    .flow-node:nth-child(2)::before, .flow-node:nth-child(3)::before { background: var(--amber); }
    .flow-node:nth-child(4)::before, .flow-node:nth-child(5)::before { background: var(--cyan); }
    .flow-node:nth-child(6)::before, .flow-node:nth-child(7)::before { background: var(--green); }
    .flow-node:not(:last-child)::after { content: "→"; position: absolute; right: 10px; top: 10px; color: #94a3b8; font-weight: 800; }
    .flow-node strong { color: var(--ink); font-size: 14px; }
    .flow-node span { color: var(--muted); font-size: 12px; line-height: 1.45; }
    .flow-num { width: 28px; height: 28px; border-radius: 999px; background: var(--soft-blue); color: var(--blue); display: grid; place-items: center; font-size: 12px; font-weight: 800; }
    .pipeline { padding: 14px 16px 16px; display: grid; gap: 10px; }
    .pipeline-step { display: grid; grid-template-columns: 34px 1fr auto; gap: 10px; align-items: center; padding: 10px 11px; border: 1px solid var(--line); border-radius: 8px; background: #fbfcfd; }
    .step-index { width: 28px; height: 28px; display: grid; place-items: center; border-radius: 50%; background: var(--soft-blue); color: var(--blue); font-weight: 800; font-size: 12px; }
    .pipeline-step strong { font-size: 13px; color: var(--ink); }
    .pipeline-step span { font-size: 12px; color: var(--muted); }
    .model-compare { padding: 14px 16px 16px; display: grid; gap: 12px; }
    .model-bar { display: grid; gap: 6px; }
    .bar-head { display: flex; justify-content: space-between; gap: 8px; font-size: 12px; color: #425466; }
    .bar-track { height: 10px; border-radius: 999px; background: #edf2f7; overflow: hidden; }
    .bar-fill { height: 100%; border-radius: 999px; background: var(--blue); }
    .bar-fill.candidate { background: var(--cyan); }
    .impact-grid { padding: 14px 16px 16px; display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
    .impact-item { min-height: 88px; border: 1px solid var(--line); border-radius: 8px; padding: 11px; background: #fbfcfd; display: grid; align-content: space-between; }
    .impact-item strong { color: var(--ink); font-size: 13px; }
    .impact-item span { color: var(--muted); font-size: 12px; line-height: 1.4; }
    .impact-item b { color: var(--cyan); font-size: 18px; }
    .viz-panel { padding: 14px 16px 16px; display: grid; gap: 12px; }
    .viz-summary { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }
    .viz-card { border: 1px solid var(--line); border-radius: 8px; background: #fbfcfd; padding: 10px 11px; min-height: 82px; display: grid; align-content: space-between; position: relative; overflow: hidden; }
    .viz-card::before { content: ""; position: absolute; inset: 0 auto 0 0; width: 4px; background: var(--blue); }
    .viz-card.good::before { background: var(--green); }
    .viz-card.warn::before { background: var(--amber); }
    .viz-card.danger::before { background: var(--red); }
    .viz-card span { color: var(--muted); font-size: 12px; }
    .viz-card strong { color: var(--ink); font-size: 19px; line-height: 1.15; }
    .viz-card small { color: var(--muted); font-size: 12px; line-height: 1.35; }
    .canvas-wrap { min-height: 0; }
    canvas { width: 100%; max-width: 100%; height: auto; display: block; border: 1px solid var(--line); border-radius: 8px; background: #fbfcfd; cursor: zoom-in; }
    .risk-board, .flow-grid { display: grid; gap: 8px; }
    .risk-row { display: grid; grid-template-columns: minmax(105px, .9fr) minmax(150px, 1.4fr) minmax(62px, .45fr); gap: 10px; align-items: center; padding: 9px 10px; border: 1px solid var(--line); border-radius: 8px; background: #fbfcfd; }
    .risk-name { color: var(--ink); font-size: 12px; font-weight: 700; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .risk-meta { color: var(--muted); font-size: 12px; text-align: right; font-variant-numeric: tabular-nums; }
    .stack-strip { height: 12px; border-radius: 999px; overflow: hidden; display: flex; background: #edf2f7; }
    .stack-fill { min-width: 2px; }
    .stack-fill.success { background: var(--green); }
    .stack-fill.failed { background: var(--red); opacity: .9; }
    .flow-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    .flow-tile { border: 1px solid var(--line); border-radius: 8px; background: #fbfcfd; padding: 11px; display: grid; gap: 8px; min-height: 104px; }
    .flow-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
    .flow-head strong { color: var(--ink); font-size: 13px; }
    .flow-dot { width: 11px; height: 11px; border-radius: 50%; background: var(--blue); box-shadow: 0 0 0 4px var(--soft-blue); flex: 0 0 auto; }
    .flow-tile:nth-child(2) .flow-dot { background: var(--amber); box-shadow: 0 0 0 4px var(--soft-amber); }
    .flow-tile:nth-child(3) .flow-dot { background: var(--cyan); box-shadow: 0 0 0 4px #eaf8fa; }
    .flow-tile b { color: var(--ink); font-size: 20px; font-variant-numeric: tabular-nums; }
    .flow-tile span { color: var(--muted); font-size: 12px; line-height: 1.4; }
    table { width: 100%; border-collapse: collapse; }
    th, td { padding: 10px 12px; border-bottom: 1px solid var(--line); text-align: left; font-size: 13px; vertical-align: top; }
    th { color: #445466; font-weight: 700; background: #f8fafc; }
    td.num { text-align: right; font-variant-numeric: tabular-nums; }
    .table-wrap { overflow-x: auto; }
    .alerts, .agents, .issues, .models, .steps { padding: 12px 16px 16px; display: grid; gap: 10px; }
    .alert, .agent, .issue, .model, .step { border: 1px solid var(--line); border-radius: 8px; padding: 11px 12px; background: #fbfcfd; }
    .alert strong, .agent strong, .issue strong, .model strong, .step strong { display: block; font-size: 13px; }
    .alert p, .agent p, .issue p, .model p, .step p { margin: 5px 0 0; color: var(--muted); font-size: 12px; line-height: 1.45; }
    .feedback-grid { padding: 14px 16px 16px; display: grid; gap: 12px; }
    .feedback-cards { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; }
    .feedback-card { border: 1px solid var(--line); border-radius: 8px; background: #fbfcfd; padding: 11px 12px; min-height: 92px; display: grid; align-content: space-between; gap: 6px; position: relative; overflow: hidden; }
    .feedback-card::before { content: ""; position: absolute; inset: 0 auto 0 0; width: 4px; background: var(--blue); }
    .feedback-card.good::before { background: var(--green); }
    .feedback-card.warn::before { background: var(--amber); }
    .feedback-card.danger::before { background: var(--red); }
    .feedback-card span { color: var(--muted); font-size: 12px; }
    .feedback-card strong { color: var(--ink); font-size: 18px; line-height: 1.15; font-variant-numeric: tabular-nums; }
    .feedback-card small { color: var(--muted); font-size: 12px; line-height: 1.35; }
    .feedback-list { display: grid; gap: 8px; }
    .feedback-row { border: 1px solid var(--line); border-radius: 8px; background: #fbfcfd; padding: 10px 11px; display: grid; grid-template-columns: minmax(170px, 1.15fr) minmax(150px, .95fr) minmax(70px, .35fr); gap: 10px; align-items: center; }
    .feedback-row strong { color: var(--ink); font-size: 13px; }
    .feedback-row span { color: var(--muted); font-size: 12px; line-height: 1.35; }
    .feedback-row b { color: var(--cyan); font-size: 16px; font-variant-numeric: tabular-nums; text-align: right; }
    .split-bars { display: grid; gap: 8px; }
    .split-bar { display: grid; grid-template-columns: 96px 1fr 70px; gap: 8px; align-items: center; color: var(--muted); font-size: 12px; }
    .split-track { height: 10px; border-radius: 999px; background: #edf2f7; overflow: hidden; }
    .split-fill { height: 100%; border-radius: 999px; background: var(--blue); }
    .split-fill.validation { background: var(--cyan); }
    .split-fill.gray { background: var(--green); }
    .split-fill.holdout { background: var(--amber); }
    .split-fill.blocked { background: var(--red); }
    .badge { display: inline-flex; align-items: center; min-height: 22px; padding: 2px 7px; border-radius: 999px; font-size: 12px; background: #edf2f7; color: #36495d; }
    .badge.critical { color: var(--red); background: #fff0f0; }
    .badge.warning { color: var(--amber); background: #fff6e6; }
    .badge.info { color: var(--cyan); background: #eaf8fa; }
    .chat { display: grid; grid-template-rows: auto minmax(260px, 48vh) auto; min-height: 620px; }
    .messages { padding: 14px; overflow: auto; background: #f8fafc; }
    .msg { max-width: 92%; margin: 0 0 10px; padding: 10px 11px; border-radius: 8px; line-height: 1.5; font-size: 14px; white-space: pre-wrap; }
    .msg.user { margin-left: auto; background: #e8f0ff; border: 1px solid #c8d8ff; }
    .msg.agent { background: white; border: 1px solid var(--line); }
    .chat-form { display: grid; grid-template-columns: 1fr auto; gap: 10px; padding: 12px; border-top: 1px solid var(--line); }
    .chat-form textarea { resize: vertical; min-height: 42px; max-height: 130px; border: 1px solid var(--line); border-radius: 6px; padding: 10px; }
    .quick { display: flex; flex-wrap: wrap; gap: 8px; padding: 0 12px 12px; }
    .quick button { font-size: 12px; padding: 6px 8px; }
    .report { margin-top: 18px; }
    pre { margin: 0; padding: 16px; overflow: auto; white-space: pre-wrap; color: #25313f; font-size: 13px; line-height: 1.55; }
    .status-line { color: var(--muted); font-size: 12px; }
    .interaction { padding: 14px 16px 16px; display: grid; gap: 12px; }
    .confirm-card { border: 1px solid var(--line); border-radius: 8px; padding: 14px; background: #fbfcfd; min-height: 132px; }
    .confirm-card h3 { margin: 0 0 8px; font-size: 15px; }
    .confirm-card p { margin: 5px 0; color: var(--muted); font-size: 13px; line-height: 1.45; }
    .confirm-actions { display: flex; flex-wrap: wrap; gap: 8px; }
    .btn.yes { background: var(--green); border-color: var(--green); color: #fff; }
    .btn.no { background: var(--red); border-color: var(--red); color: #fff; }
    .event-log { max-height: 260px; border: 1px solid var(--line); border-radius: 8px; background: #111827; color: #d7e0ea; }
    .online-form { display: grid; grid-template-columns: repeat(4, minmax(120px, 1fr)) repeat(3, auto); gap: 10px; align-items: end; }
    .field label { display: block; color: var(--muted); font-size: 12px; margin-bottom: 5px; }
    .field input { width: 100%; min-height: 38px; padding: 8px 10px; border: 1px solid var(--line); border-radius: 6px; background: #fff; }
    .online-result { border: 1px solid var(--line); border-radius: 8px; overflow: hidden; }
    .online-viz { display: grid; gap: 14px; }
    .online-viz-head { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; }
    .curve-card { border: 1px solid var(--line); border-radius: 8px; background: #fbfcfd; padding: 10px 11px; display: grid; gap: 5px; min-height: 76px; }
    .curve-card span { color: var(--muted); font-size: 12px; }
    .curve-card strong { color: var(--ink); font-size: 18px; font-variant-numeric: tabular-nums; }
    .curve-card small { color: var(--muted); font-size: 12px; line-height: 1.35; }
    .curve-toolbar { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; justify-content: space-between; }
    .chart-actions { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; justify-content: flex-end; }
    .curve-models { display: flex; flex-wrap: wrap; gap: 8px; }
    .model-chip { border: 1px solid var(--line); background: #fff; color: #425466; border-radius: 999px; padding: 5px 9px; font-size: 12px; cursor: pointer; }
    .model-chip.active { background: var(--blue); border-color: var(--blue); color: #fff; }
    .curve-legend { display: flex; flex-wrap: wrap; gap: 12px; color: var(--muted); font-size: 12px; }
    .legend-item { display: inline-flex; align-items: center; gap: 6px; }
    .legend-line { width: 24px; height: 3px; border-radius: 999px; background: var(--blue); }
    .legend-line.before { background: var(--amber); }
    .legend-line.after { background: var(--green); }
    .legend-line.gain { background: rgba(19, 138, 85, .24); height: 10px; border: 1px solid rgba(19, 138, 85, .34); }
    .legend-line.baseline { background: var(--blue); }
    .legend-line.iteration { background: var(--cyan); }
    .iteration-stage { border: 1px solid var(--line); border-radius: 8px; padding: 12px; background: #fbfcfd; display: grid; gap: 12px; }
    .comparison-grid { display: grid; grid-template-columns: 1fr; gap: 12px; align-items: stretch; }
    .compare-cards { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; }
    .compare-card { border: 1px solid var(--line); border-radius: 8px; background: #fbfcfd; padding: 12px; min-height: 104px; display: grid; align-content: space-between; gap: 8px; position: relative; overflow: hidden; }
    .compare-card::before { content: ""; position: absolute; left: 0; top: 0; bottom: 0; width: 4px; background: var(--blue); }
    .compare-card.iteration::before { background: var(--cyan); }
    .compare-card.delta::before { background: var(--green); }
    .compare-card.warn::before { background: var(--amber); }
    .compare-card span { color: var(--muted); font-size: 12px; }
    .compare-card strong { color: var(--ink); font-size: 20px; font-variant-numeric: tabular-nums; line-height: 1.15; }
    .compare-card small { color: var(--muted); font-size: 12px; line-height: 1.35; }
    .zoom-overlay { position: fixed; inset: 0; z-index: 50; display: none; align-items: center; justify-content: center; padding: 22px; background: rgba(15, 23, 42, .72); }
    .zoom-overlay.active { display: flex; }
    .zoom-modal { width: min(98vw, 1680px); max-height: 94vh; border-radius: 8px; background: #ffffff; border: 1px solid #cbd5e1; box-shadow: 0 24px 70px rgba(15, 23, 42, .32); display: grid; grid-template-rows: auto minmax(0, 1fr); overflow: hidden; }
    .zoom-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 11px 14px; border-bottom: 1px solid var(--line); }
    .zoom-head strong { color: var(--ink); font-size: 14px; }
    .zoom-body { padding: 14px; overflow: auto; background: #f8fafc; }
    .zoom-body img { width: 100%; height: auto; display: block; border: 1px solid var(--line); border-radius: 8px; background: #fbfcfd; }
    @media (max-width: 1180px) {
      .hero-panel, .layout, .split, .viz-split, .triple, .hero-visual, .system-flow { grid-template-columns: 1fr; }
      .flow-track { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .hero-facts { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .mini-fact:nth-child(2) { border-right: 0; }
      .metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .proof-strip { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .online-form { grid-template-columns: 1fr 1fr; }
      .impact-grid { grid-template-columns: 1fr 1fr; }
      .viz-summary, .flow-grid { grid-template-columns: 1fr; }
      .feedback-cards { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .online-viz-head { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .comparison-grid { grid-template-columns: 1fr; }
      .chat { min-height: 520px; }
    }
    @media (max-width: 640px) {
      .topbar-inner { align-items: stretch; flex-direction: column; padding: 12px; }
      main { padding: 12px; }
      .metrics { grid-template-columns: 1fr; }
      .brand { min-width: 0; }
      .actions { justify-content: stretch; }
      .actions .btn { flex: 1 1 auto; }
      .proof-strip { grid-template-columns: 1fr; }
      .flow-track { grid-template-columns: 1fr; }
      .hero-facts { grid-template-columns: 1fr; }
      .mini-fact { border-right: 0; border-bottom: 1px solid var(--line); }
      .mini-fact:last-child { border-bottom: 0; }
      .online-form { grid-template-columns: 1fr; }
      .impact-grid { grid-template-columns: 1fr; }
      .feedback-cards { grid-template-columns: 1fr; }
      .feedback-row { grid-template-columns: 1fr; }
      .feedback-row b { text-align: left; }
      .split-bar { grid-template-columns: 1fr; }
      .risk-row { grid-template-columns: 1fr; }
      .risk-meta { text-align: left; }
      .online-viz-head { grid-template-columns: 1fr; }
      .compare-cards { grid-template-columns: 1fr; }
      th, td { padding: 9px 8px; }
    }
  </style>
</head>
<body>
  <div class="topbar">
    <div class="topbar-inner">
      <div class="brand">
        <h1>光擎 SolarRestore Agent</h1>
        <p>全球光伏削峰还原自进化智能体平台</p>
      </div>
      <div class="actions">
        <button class="btn" id="refreshBtn">刷新监控</button>
        <button class="btn" id="simulateBtn">模拟日终运行</button>
        <button class="btn" id="onlineBtn">读取线上9030</button>
        <button class="btn" id="k8sBtn">分析K8s日志</button>
        <button class="btn" id="createConfirmBtn">发送确认卡片</button>
        <button class="btn primary" id="sendFeishuBtn">发送飞书告警</button>
      </div>
    </div>
  </div>
  <main>
    <section class="hero-panel">
      <div class="hero-copy">
        <div class="eyebrow">AI Business Competition Demo</div>
        <h2>光擎 SolarRestore Agent</h2>
        <p>面向全球光伏站点的削峰还原 AI 闭环：每天按本地时区自动识别削峰、判断场景、双模型还原、Kafka 下发、日终归因，并把线上问题沉淀为下一版数据集和模型。</p>
        <div class="hero-badges">
          <span class="hero-badge">生产链路已部署</span>
          <span class="hero-badge">StarRocks 9030 实时读取</span>
          <span class="hero-badge">K8s 容器日志分析</span>
          <span class="hero-badge">飞书 Agent 交互</span>
          <span class="hero-badge">双模型灰度并行</span>
        </div>
      </div>
      <div class="hero-visual">
        <div class="visual-card">
          <svg viewBox="0 0 520 300" role="img" aria-label="户用光伏发电场景">
            <defs>
              <linearGradient id="pvSky" x1="0" y1="0" x2="1" y2="1">
                <stop offset="0" stop-color="#eaf8fa" />
                <stop offset="1" stop-color="#fff7e6" />
              </linearGradient>
              <linearGradient id="pvPanel" x1="0" y1="0" x2="1" y2="1">
                <stop offset="0" stop-color="#1b4d89" />
                <stop offset="1" stop-color="#2f6fed" />
              </linearGradient>
            </defs>
            <rect width="520" height="300" fill="url(#pvSky)" />
            <circle cx="424" cy="64" r="34" fill="#f4b740" opacity=".9" />
            <path d="M0 226 C110 190 168 232 274 202 C372 174 432 206 520 184 L520 300 L0 300 Z" fill="#dff4e8" />
            <path d="M104 172 L244 92 L384 172 L384 252 L104 252 Z" fill="#ffffff" stroke="#cbd5e1" stroke-width="3" />
            <path d="M84 176 L244 78 L404 176 Z" fill="#334155" />
            <path d="M150 156 L242 102 L334 156 L310 176 L174 176 Z" fill="url(#pvPanel)" stroke="#dbeafe" stroke-width="2" />
            <path d="M178 143 L316 143 M202 128 L292 128 M214 112 L270 112 M194 158 L286 106 M236 176 L328 132" stroke="#b9d6ff" stroke-width="2" opacity=".8" />
            <rect x="138" y="202" width="54" height="50" rx="4" fill="#eaf1ff" stroke="#cbd5e1" />
            <rect x="264" y="198" width="78" height="40" rx="5" fill="#f8fafc" stroke="#cbd5e1" />
            <path d="M60 232 L450 232" stroke="#94a3b8" stroke-width="3" stroke-linecap="round" opacity=".5" />
            <path d="M398 142 C430 128 455 135 474 158" stroke="#138a55" stroke-width="4" fill="none" stroke-linecap="round" />
            <path d="M398 142 C420 168 448 176 480 168" stroke="#138a55" stroke-width="4" fill="none" stroke-linecap="round" opacity=".8" />
          </svg>
          <div class="visual-caption"><strong>户用光伏真实能力还原</strong><span>削峰后观测曲线被压平，Agent 还原真实发电上限。</span></div>
        </div>
        <div class="visual-card">
          <svg viewBox="0 0 520 300" role="img" aria-label="全球化光伏站点调度">
            <defs>
              <linearGradient id="globalBg" x1="0" y1="0" x2="1" y2="1">
                <stop offset="0" stop-color="#eef6ff" />
                <stop offset="1" stop-color="#eaf7f0" />
              </linearGradient>
            </defs>
            <rect width="520" height="300" fill="url(#globalBg)" />
            <circle cx="260" cy="142" r="96" fill="#ffffff" stroke="#cbd5e1" stroke-width="3" />
            <ellipse cx="260" cy="142" rx="96" ry="38" fill="none" stroke="#93c5fd" stroke-width="3" />
            <ellipse cx="260" cy="142" rx="48" ry="96" fill="none" stroke="#93c5fd" stroke-width="3" />
            <path d="M178 100 C224 116 282 118 342 96 M176 184 C232 166 294 166 344 186" stroke="#93c5fd" stroke-width="3" fill="none" />
            <path d="M142 118 C118 132 98 154 82 184 M378 104 C420 118 450 148 468 190 M170 224 C126 222 94 204 70 172 M350 222 C404 222 448 204 480 168" stroke="#087f8c" stroke-width="2.5" fill="none" stroke-dasharray="6 7" opacity=".85" />
            <g fill="#2f6fed">
              <circle cx="168" cy="110" r="8" />
              <circle cx="320" cy="96" r="8" />
              <circle cx="344" cy="190" r="8" />
              <circle cx="214" cy="202" r="8" />
            </g>
            <g fill="#138a55">
              <circle cx="82" cy="184" r="7" />
              <circle cx="468" cy="190" r="7" />
              <circle cx="480" cy="168" r="7" />
              <circle cx="70" cy="172" r="7" />
            </g>
            <rect x="202" y="126" width="116" height="48" rx="10" fill="#17212f" opacity=".9" />
            <path d="M222 154 L244 154 L252 138 L266 164 L276 148 L288 154 L300 154" stroke="#6ee7b7" stroke-width="4" fill="none" stroke-linecap="round" stroke-linejoin="round" />
          </svg>
          <div class="visual-caption"><strong>全球区域本地时区调度</strong><span>按区域 19:45 自动运行，万级站点统一归因和灰度。</span></div>
        </div>
        <div class="hero-facts">
          <div class="mini-fact"><span>生产入口</span><strong>main_test_2.py</strong></div>
          <div class="mini-fact"><span>调度时间</span><strong>区域本地 19:45</strong></div>
          <div class="mini-fact"><span>下发通道</span><strong>PV_RESULT_TOPIC</strong></div>
          <div class="mini-fact"><span>参赛主题</span><strong>生产自动运行 -> 自进化闭环</strong></div>
        </div>
      </div>
    </section>
    <div class="proof-strip">
      <div class="proof-item">
        <div class="proof-top"><span>线上可信度</span><span class="proof-icon">DB</span></div>
        <strong>StarRocks 9030 只读接入</strong>
        <p>按 dt + predict_type=4 统计削峰还原入库站点，支持真实线上库验证。</p>
      </div>
      <div class="proof-item">
        <div class="proof-top"><span>分析自由度</span><span class="proof-icon">ID</span></div>
        <strong>任意站点 + 日期查询</strong>
        <p>输入 station_id 和日期即可查看单站明细，同时支持留空查询全站。</p>
      </div>
      <div class="proof-item">
        <div class="proof-top"><span>Agent 交互</span><span class="proof-icon">FS</span></div>
        <strong>飞书卡片 Yes / No 闭环</strong>
        <p>Agent 发卡片、用户点击、WebSocket 回调、上下文状态更新。</p>
      </div>
      <div class="proof-item">
        <div class="proof-top"><span>模型进化</span><span class="proof-icon">AI</span></div>
        <strong>双模型灰度晋升机制</strong>
        <p>原模型与 10000 站候选模型复用同一份输入并行评估。</p>
      </div>
    </div>
    <div class="layout">
      <div>
        <section>
          <div class="section-head">
            <h2>欧洲线上样例监控</h2>
            <span id="generatedAt"></span>
          </div>
          <div class="metrics" id="metrics"></div>
        </section>
        <section>
          <div class="section-head">
            <h2>生产智能体流程总览</h2>
            <span>从每日自动运行到模型自进化</span>
          </div>
          <div class="system-flow">
            <div class="flow-art-panel">
              <svg viewBox="0 0 420 320" role="img" aria-label="全球光伏智能体闭环">
                <defs>
                  <linearGradient id="flowBg" x1="0" y1="0" x2="1" y2="1">
                    <stop offset="0" stop-color="#eff6ff" />
                    <stop offset="1" stop-color="#ecfdf5" />
                  </linearGradient>
                </defs>
                <rect width="420" height="320" fill="url(#flowBg)" />
                <circle cx="208" cy="126" r="78" fill="#ffffff" stroke="#cbd5e1" stroke-width="3" />
                <ellipse cx="208" cy="126" rx="78" ry="28" fill="none" stroke="#93c5fd" stroke-width="3" />
                <ellipse cx="208" cy="126" rx="36" ry="78" fill="none" stroke="#93c5fd" stroke-width="3" />
                <path d="M90 214 C154 178 260 178 330 214" stroke="#087f8c" stroke-width="4" fill="none" stroke-linecap="round" />
                <path d="M112 230 L154 202 L196 230 L196 268 L112 268 Z" fill="#ffffff" stroke="#cbd5e1" stroke-width="2" />
                <path d="M102 232 L154 194 L206 232 Z" fill="#334155" />
                <path d="M132 222 L154 206 L176 222 Z" fill="#2f6fed" />
                <rect x="262" y="224" width="70" height="42" rx="5" fill="#17212f" opacity=".9" />
                <path d="M276 246 L288 246 L294 234 L304 258 L310 246 L324 246" stroke="#6ee7b7" stroke-width="4" fill="none" stroke-linecap="round" />
                <circle cx="136" cy="86" r="7" fill="#2f6fed" />
                <circle cx="270" cy="76" r="7" fill="#087f8c" />
                <circle cx="300" cy="156" r="7" fill="#138a55" />
                <circle cx="154" cy="168" r="7" fill="#b46904" />
              </svg>
              <div class="flow-art-copy"><strong>全球光伏削峰还原闭环</strong><span>从站点数据到模型晋升，所有关键动作都可监控、可归因、可回流。</span></div>
            </div>
            <div class="flow-track">
              <div class="flow-node"><div class="flow-num">1</div><strong>数据接入</strong><span>StarRocks 功率、MySQL 配置、NWP 气象对齐到 288 点。</span></div>
              <div class="flow-node"><div class="flow-num">2</div><strong>削峰识别</strong><span>聚合初筛与明细复筛，识别潜在削峰站点。</span></div>
              <div class="flow-node"><div class="flow-num">3</div><strong>场景判定</strong><span>输出 PCS、电网/卖电限制、负载侧约束等场景标签。</span></div>
              <div class="flow-node"><div class="flow-num">4</div><strong>双模型还原</strong><span>第一版与 10000station 迭代版共享输入并行推理。</span></div>
              <div class="flow-node"><div class="flow-num">5</div><strong>Kafka 下发</strong><span>将还原后的 PV 能力结果下发到生产主题。</span></div>
              <div class="flow-node"><div class="flow-num">6</div><strong>日终归因</strong><span>汇总区域成功率、失败原因和样本回流价值。</span></div>
              <div class="flow-node"><div class="flow-num">7</div><strong>灰度晋升</strong><span>按线上指标评估候选模型，达标后晋升主模型。</span></div>
            </div>
          </div>
        </section>
        <div class="triple">
          <section>
            <div class="section-head">
              <h2>AI Agent 闭环链路</h2>
              <span>自动运行 -> 自动进化</span>
            </div>
            <div class="pipeline" id="pipelineSteps"></div>
          </section>
          <section>
            <div class="section-head">
              <h2>双模型灰度表现</h2>
              <span>同输入并行推理</span>
            </div>
            <div class="model-compare" id="modelCompare"></div>
          </section>
        </div>
        <div class="split viz-split">
          <section>
            <div class="section-head">
              <h2>区域处理表现</h2>
              <div class="head-actions"><span>潜在削峰 / 成功率</span><button class="btn mini" id="zoomRegionBtn" type="button">放大</button></div>
            </div>
            <div class="viz-panel">
              <div class="viz-summary" id="regionInsightCards"></div>
              <div class="canvas-wrap">
                <canvas id="regionChart" width="1400" height="620"></canvas>
              </div>
              <div class="risk-board" id="regionRiskBoard"></div>
            </div>
          </section>
          <section>
            <div class="section-head">
              <h2>问题归因 Pareto</h2>
              <div class="head-actions"><span>日终分析 Agent</span><button class="btn mini" id="zoomIssueBtn" type="button">放大</button></div>
            </div>
            <div class="viz-panel">
              <div class="viz-summary" id="issueInsightCards"></div>
              <div class="canvas-wrap">
                <canvas id="issueChart" width="1400" height="620"></canvas>
              </div>
              <div class="flow-grid" id="issueFlowGrid"></div>
            </div>
          </section>
        </div>
        <div class="split">
          <section>
            <div class="section-head">
              <h2>重点区域明细</h2>
              <span>真实日志统计</span>
            </div>
            <div class="table-wrap">
              <table>
                <thead>
                  <tr><th>区域</th><th>总站点</th><th>潜在</th><th>成功率</th><th>失败/跳过</th></tr>
                </thead>
                <tbody id="regionRows"></tbody>
              </table>
            </div>
          </section>
          <section>
            <div class="section-head">
              <h2>参赛价值评分卡</h2>
              <span>AI 业务赛道表达</span>
            </div>
            <div class="impact-grid">
              <div class="impact-item"><strong>AI 重构流程</strong><b>端到端</b><span>从人工排查升级为 Agent 自动运行、归因、回流和灰度。</span></div>
              <div class="impact-item"><strong>生产级落地</strong><b>5 类系统</b><span>StarRocks、MySQL、Kafka、APScheduler、S3 已在链路中使用。</span></div>
              <div class="impact-item"><strong>万级站点规模</strong><b>29,456</b><span>欧洲单日线上日志样例覆盖近三万站点。</span></div>
              <div class="impact-item"><strong>持续进化</strong><b>闭环</b><span>线上失败样本驱动数据集、模型和发布策略迭代。</span></div>
            </div>
          </section>
        </div>
        <div class="split">
          <section>
            <div class="section-head">
              <h2>自动告警</h2>
              <span>分析 Agent 输出</span>
            </div>
            <div class="alerts" id="alerts"></div>
          </section>
          <section>
            <div class="section-head">
              <h2>问题归因与回流动作</h2>
              <span>Top buckets</span>
            </div>
            <div class="issues" id="issues"></div>
          </section>
        </div>
        <div class="split">
          <section>
            <div class="section-head">
              <h2>高价值样本回流</h2>
              <span id="feedbackStatus">问题样本驱动迭代</span>
            </div>
            <div class="feedback-grid">
              <div class="feedback-cards" id="feedbackCards"></div>
              <div class="feedback-list" id="feedbackBuckets"></div>
            </div>
          </section>
          <section>
            <div class="section-head">
              <h2>数据集与场景标签重构</h2>
              <span id="datasetStatus">样本池 -> 新数据集版本</span>
            </div>
            <div class="feedback-grid">
              <div class="feedback-cards" id="datasetCards"></div>
              <div class="split-bars" id="datasetSplits"></div>
              <div class="feedback-list" id="sceneLabels"></div>
            </div>
          </section>
        </div>
        <div class="split">
          <section>
            <div class="section-head">
              <h2>双模型灰度</h2>
              <span>共享同一份输入</span>
            </div>
            <div class="models" id="models"></div>
          </section>
          <section>
            <div class="section-head">
              <h2>Agent 闭环进度</h2>
              <span>生产 -> 进化</span>
            </div>
            <div class="steps" id="steps"></div>
          </section>
        </div>
        <section class="report">
          <div class="section-head">
            <h2>线上 StarRocks 9030 数据</h2>
            <span id="onlineStatus">按 dt + predict_type=4 统计去重站点</span>
          </div>
          <div class="interaction">
            <div class="online-form">
              <div class="field">
                <label for="onlineDate">日期 dt</label>
                <input id="onlineDate" type="date" value="2026-05-22" />
              </div>
              <div class="field">
                <label for="onlineStation">站点 station_id，可任意填写</label>
                <input id="onlineStation" value="2026021600036" placeholder="留空则查询当天全站" />
              </div>
              <div class="field">
                <label for="onlineModel">模型名，可空</label>
                <input id="onlineModel" placeholder="PowerRestorationModel" />
              </div>
              <div class="field">
                <label for="onlineLimit">明细行数</label>
                <input id="onlineLimit" value="20" />
              </div>
              <button class="btn primary" id="loadOnlineBtn" type="button">查询</button>
              <button class="btn" id="queryAllOnlineBtn" type="button">查询全站</button>
              <button class="btn" id="sampleOnlineBtn" type="button">示例站点</button>
            </div>
            <div class="online-result">
              <pre id="onlineResult">选择任意日期和站点后点击“查询”。站点留空时统计该日期全站削峰还原入库数。</pre>
            </div>
            <div class="online-viz">
              <div class="curve-toolbar">
                <div class="curve-legend">
                  <span class="legend-item"><span class="legend-line before"></span>还原前观测 PV</span>
                  <span class="legend-item"><span class="legend-line after"></span>还原后真实能力</span>
                  <span class="legend-item"><span class="legend-line gain"></span>多发电量面积</span>
                </div>
                <div class="chart-actions">
                  <div class="curve-models" id="curveModels"></div>
                  <button class="btn" id="zoomRestorationBtn" type="button">放大曲线</button>
                </div>
              </div>
              <div class="online-viz-head" id="curveCards">
                <div class="curve-card"><span>曲线状态</span><strong>待查询</strong><small>选择站点后自动读取原始功率和还原结果。</small></div>
              </div>
              <div class="canvas-wrap">
                <canvas id="restorationCurveChart" width="1400" height="560"></canvas>
              </div>
              <div class="iteration-stage">
                <div class="curve-toolbar">
                  <div class="curve-legend">
                    <span class="legend-item"><span class="legend-line baseline"></span>Restoration 第一版</span>
                    <span class="legend-item"><span class="legend-line iteration"></span>10000station 迭代版</span>
                    <span class="legend-item"><span class="legend-line gain"></span>多发电量对比</span>
                  </div>
                  <button class="btn" id="zoomCompareBtn" type="button">放大迭代效果图</button>
                </div>
                <div class="comparison-grid">
                  <div class="canvas-wrap">
                    <canvas id="modelEnergyCompareChart" width="1680" height="760"></canvas>
                  </div>
                  <div class="compare-cards" id="modelCompareCards">
                    <div class="compare-card"><span>模型对比</span><strong>待查询</strong><small>查询站点后展示第一版与迭代版多发电量。</small></div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </section>
        <section class="report">
          <div class="section-head">
            <h2>欧洲 K8s 实时日志</h2>
            <span id="k8sStatus">Dashboard 只读日志分析</span>
          </div>
          <div class="interaction">
            <div class="online-form">
              <div class="field">
                <label for="k8sNamespace">namespace</label>
                <input id="k8sNamespace" value="data-platform" />
              </div>
              <div class="field">
                <label for="k8sKeywords">关键词，逗号或空格分隔</label>
                <input id="k8sKeywords" value="sigen-pv-clipping" />
              </div>
              <div class="field">
                <label for="k8sTailLines">每个容器 tail 行数</label>
                <input id="k8sTailLines" value="500" />
              </div>
              <div class="field">
                <label for="k8sLimitPods">最多 Pod 数</label>
                <input id="k8sLimitPods" value="8" />
              </div>
              <button class="btn primary" id="loadK8sBtn" type="button">分析</button>
              <button class="btn" id="k8sClippingBtn" type="button">sigen-pv-clipping</button>
              <button class="btn" id="k8sDataPlatformBtn" type="button">data-platform</button>
            </div>
            <div class="online-result">
              <pre id="k8sResult">设置 PV_CLIPPING_K8S_TOKEN 后点击“分析”，系统会读取 data-platform / sigen-pv-clipping 的 Pod 日志并自动归因。</pre>
            </div>
          </div>
        </section>
        <section class="report">
          <div class="section-head">
            <h2>日终报告</h2>
            <span id="reportStatus">待生成</span>
          </div>
          <pre id="reportText">点击“模拟日终运行”生成可发送到飞书的报告。</pre>
        </section>
        <section class="report">
          <div class="section-head">
            <h2>飞书卡片交互闭环</h2>
            <span id="wsStatus">WebSocket 未连接</span>
          </div>
          <div class="interaction">
            <div class="confirm-card" id="confirmCard">
              <h3>尚未创建确认任务</h3>
              <p>点击“发送确认卡片”后，Agent 会生成一张 Yes/No 卡片。这里模拟用户在飞书里点击按钮，服务会收到回调并更新原卡片状态。</p>
            </div>
            <div class="confirm-actions">
              <button class="btn yes" id="simulateYesBtn" type="button">模拟点击 Yes</button>
              <button class="btn no" id="simulateNoBtn" type="button">模拟点击 No</button>
              <button class="btn" id="duplicateClickBtn" type="button">模拟重复点击</button>
            </div>
            <pre class="event-log" id="eventLog">等待 WebSocket 事件...</pre>
          </div>
        </section>
      </div>
      <section class="chat">
        <div class="section-head">
          <h2>飞书 Agent 提问模拟</h2>
          <span>同一套逻辑可用于 /feishu/events 回调</span>
        </div>
        <div class="messages" id="messages"></div>
        <form class="chat-form" id="chatForm">
          <textarea id="question" placeholder="例如：今天欧洲削峰还原状态如何？"></textarea>
          <button class="btn primary" type="submit">提问</button>
        </form>
        <div class="quick" id="quickQuestions"></div>
      </section>
    </div>
  </main>
  <div class="zoom-overlay" id="chartZoomOverlay" role="dialog" aria-modal="true" aria-hidden="true">
    <div class="zoom-modal">
      <div class="zoom-head">
        <strong id="zoomTitle">图表放大</strong>
        <button class="btn" id="closeZoomBtn" type="button">关闭</button>
      </div>
      <div class="zoom-body">
        <img id="zoomImage" alt="放大后的可视化图表" />
      </div>
    </div>
  </div>
  <script>
    const fmt = new Intl.NumberFormat('zh-CN');
    let snapshot = null;
    let currentTask = null;
    let lastChoice = 'yes';
    let lastRestorationData = null;

    async function api(path, options = {}) {
      const res = await fetch(path, {
        headers: { 'Content-Type': 'application/json' },
        ...options
      });
      if (!res.ok) throw new Error(await res.text());
      return res.json();
    }

    function pct(v) { return `${Number(v).toFixed(2)}%`; }

    function num(v) {
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    }

    function fmtPower(v) {
      const n = num(v);
      return n == null ? '-' : n.toFixed(3);
    }

    function modelStageName(modelName) {
      const name = String(modelName || '');
      return name.includes('10000station') ? '迭代版' : '第一版';
    }

    function modelDisplayName(modelName) {
      const name = String(modelName || 'model');
      if (name.includes('10000station')) return '10000station 迭代版';
      if (name.includes('PowerRestorationModel')) return 'Restoration 第一版';
      return name;
    }

    function extraEnergyOf(curve) {
      const stats = curve && curve.stats ? curve.stats : {};
      return num(stats.extra_generation_energy != null ? stats.extra_generation_energy : stats.restored_gain_energy) || 0;
    }

    function comparisonCurves(curves) {
      const list = Array.isArray(curves) ? curves : [];
      const baseline = list.find(item => !String(item.model_name || '').includes('10000station')) || null;
      const iteration = list.find(item => String(item.model_name || '').includes('10000station')) || null;
      const rows = [];
      if (baseline) rows.push({ key: 'baseline', label: 'Restoration 第一版', curve: baseline, color: '#2f6fed' });
      if (iteration) rows.push({ key: 'iteration', label: '10000station 迭代版', curve: iteration, color: '#087f8c' });
      list.forEach(item => {
        if (item !== baseline && item !== iteration) {
          rows.push({ key: 'other', label: modelDisplayName(item.model_name), curve: item, color: '#647383' });
        }
      });
      return { baseline, iteration, rows };
    }

    function metric(label, value, sub, cls = '') {
      return `<div class="metric ${cls}"><label>${label}</label><strong>${value}</strong><small>${sub}</small></div>`;
    }

    function renderMetrics(summary) {
      document.getElementById('metrics').innerHTML = [
        metric('覆盖区域', `${summary.covered_regions}`, 'Europe/*'),
        metric('覆盖站点', fmt.format(summary.total_stations), '生产日志统计'),
        metric('潜在削峰', fmt.format(summary.potential_clipping_stations), pct(summary.potential_ratio), 'warn'),
        metric('成功还原', fmt.format(summary.success_stations), pct(summary.success_rate), 'ok'),
        metric('失败/跳过', fmt.format(summary.failed_or_skipped_stations), pct(summary.failed_rate), 'danger')
      ].join('');
    }

    function renderPipeline(steps) {
      const labels = {
        running: '已上线',
        gray: '灰度中',
        planned_partial: '规划/部分实现'
      };
      document.getElementById('pipelineSteps').innerHTML = steps.map((step, idx) => `
        <div class="pipeline-step">
          <div class="step-index">${idx + 1}</div>
          <div><strong>${step.name}</strong><span>${idx < 3 ? '线上每日自动触发' : idx < 7 ? '问题样本驱动迭代' : '生产灰度与晋升'}</span></div>
          <span class="badge ${step.state === 'running' ? 'info' : step.state === 'gray' ? 'warning' : ''}">${labels[step.state] || step.state}</span>
        </div>
      `).join('');
    }

    function renderModelCompare(models) {
      const rows = [
        { name: models.baseline.name, desc: '线上基线模型入库覆盖', value: 100, cls: '' },
        { name: models.candidate.name, desc: '候选模型灰度覆盖，2026-05-22 样例 4,933 / 5,143', value: 95.92, cls: 'candidate' }
      ];
      document.getElementById('modelCompare').innerHTML = rows.map(row => `
        <div class="model-bar">
          <div class="bar-head"><strong>${row.name}</strong><span>${row.value.toFixed(2)}%</span></div>
          <div class="bar-track"><div class="bar-fill ${row.cls}" style="width:${row.value}%"></div></div>
          <div class="bar-head"><span>${row.desc}</span><span>predict_type=4</span></div>
        </div>
      `).join('');
    }

    function clamp(value, min, max) {
      return Math.max(min, Math.min(max, value));
    }

    function roundRect(ctx, x, y, width, height, radius) {
      const r = Math.min(radius, width / 2, height / 2);
      ctx.beginPath();
      ctx.moveTo(x + r, y);
      ctx.lineTo(x + width - r, y);
      ctx.quadraticCurveTo(x + width, y, x + width, y + r);
      ctx.lineTo(x + width, y + height - r);
      ctx.quadraticCurveTo(x + width, y + height, x + width - r, y + height);
      ctx.lineTo(x + r, y + height);
      ctx.quadraticCurveTo(x, y + height, x, y + height - r);
      ctx.lineTo(x, y + r);
      ctx.quadraticCurveTo(x, y, x + r, y);
      ctx.closePath();
    }

    function fillRoundRect(ctx, x, y, width, height, radius, color) {
      ctx.fillStyle = color;
      roundRect(ctx, x, y, width, height, radius);
      ctx.fill();
    }

    function strokeRoundRect(ctx, x, y, width, height, radius, color) {
      ctx.strokeStyle = color;
      roundRect(ctx, x, y, width, height, radius);
      ctx.stroke();
    }

    function drawRing(ctx, x, y, radius, value, color, label) {
      const pctValue = clamp(value, 0, 100) / 100;
      ctx.lineWidth = 5;
      ctx.strokeStyle = '#e2e8f0';
      ctx.beginPath();
      ctx.arc(x, y, radius, 0, Math.PI * 2);
      ctx.stroke();
      ctx.strokeStyle = color;
      ctx.beginPath();
      ctx.arc(x, y, radius, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * pctValue);
      ctx.stroke();
      ctx.fillStyle = '#1d2733';
      ctx.font = '700 11px Arial';
      ctx.textAlign = 'center';
      ctx.fillText(label, x, y + 4);
      ctx.textAlign = 'left';
      ctx.lineWidth = 1;
    }

    function shortText(text, size = 8) {
      return text.length > size ? `${text.slice(0, size)}...` : text;
    }

    function renderRegionInsights(regions) {
      const totalPotential = regions.reduce((sum, r) => sum + r.potential_clipping, 0);
      const totalSuccess = regions.reduce((sum, r) => sum + r.success, 0);
      const totalFailed = regions.reduce((sum, r) => sum + r.failed_or_skipped, 0);
      const avgSuccess = totalPotential ? totalSuccess / totalPotential * 100 : 0;
      const largest = [...regions].sort((a, b) => b.potential_clipping - a.potential_clipping)[0];
      const best = [...regions].sort((a, b) => b.success_rate - a.success_rate)[0];
      const risk = [...regions].sort((a, b) => b.failed_or_skipped - a.failed_or_skipped)[0];
      document.getElementById('regionInsightCards').innerHTML = [
        `<div class="viz-card warn"><span>潜在削峰总量</span><strong>${fmt.format(totalPotential)}</strong><small>${largest.area} 贡献 ${fmt.format(largest.potential_clipping)} 个潜在站点</small></div>`,
        `<div class="viz-card good"><span>加权成功率</span><strong>${pct(avgSuccess)}</strong><small>${best.area} 表现最好，成功率 ${pct(best.success_rate)}</small></div>`,
        `<div class="viz-card danger"><span>失败/跳过压力</span><strong>${fmt.format(totalFailed)}</strong><small>${risk.area} 失败/跳过 ${fmt.format(risk.failed_or_skipped)} 个</small></div>`
      ].join('');
      const topRisk = [...regions].sort((a, b) => b.failed_or_skipped - a.failed_or_skipped).slice(0, 5);
      document.getElementById('regionRiskBoard').innerHTML = topRisk.map(r => {
        const successPct = r.potential_clipping ? r.success / r.potential_clipping * 100 : 0;
        const failedPct = clamp(100 - successPct, 0, 100);
        return `
          <div class="risk-row">
            <div class="risk-name">${r.area.replace('Europe/', '')}</div>
            <div class="stack-strip" title="成功 ${fmt.format(r.success)} / 失败 ${fmt.format(r.failed_or_skipped)}">
              <div class="stack-fill success" style="width:${successPct}%"></div>
              <div class="stack-fill failed" style="width:${failedPct}%"></div>
            </div>
            <div class="risk-meta">${pct(r.success_rate)}</div>
          </div>
        `;
      }).join('');
    }

    function renderIssueInsights(issues) {
      const total = issues.reduce((sum, item) => sum + item.count, 0);
      const top = issues[0];
      const top2 = issues.slice(0, 2).reduce((sum, item) => sum + item.count, 0);
      const dataQuality = issues.filter(item => ['data_quality', 'duplicate_index', 'missing_power', 'missing_weather'].includes(item.bucket)).reduce((sum, item) => sum + item.count, 0);
      const contract = issues.filter(item => ['code_contract', 'interface_contract'].includes(item.bucket)).reduce((sum, item) => sum + item.count, 0);
      const boundary = issues.filter(item => item.bucket === 'mask_boundary').reduce((sum, item) => sum + item.count, 0);
      document.getElementById('issueInsightCards').innerHTML = [
        `<div class="viz-card danger"><span>Top 问题</span><strong>${fmt.format(top.count)}</strong><small>${top.type}，优先进入日终告警</small></div>`,
        `<div class="viz-card warn"><span>Top2 占比</span><strong>${pct(total ? top2 / total * 100 : 0)}</strong><small>功率插值与重复索引是主要瓶颈</small></div>`,
        `<div class="viz-card good"><span>可回流问题桶</span><strong>${issues.length} 类</strong><small>质量、契约、边界场景分桶沉淀</small></div>`
      ].join('');
      const tiles = [
        { title: '数据质量集', value: dataQuality, text: '功率插值、重复索引、无功率、气象空表进入质量样本池。' },
        { title: '工程契约集', value: contract, text: 'col 未定义、返回值不一致进入接口契约修复和测试。' },
        { title: '场景扩展集', value: boundary, text: '短日曲线、非 288 点、边界点削峰进入新场景标签。' }
      ];
      document.getElementById('issueFlowGrid').innerHTML = tiles.map(tile => `
        <div class="flow-tile">
          <div class="flow-head"><strong>${tile.title}</strong><span class="flow-dot"></span></div>
          <b>${fmt.format(tile.value)}</b>
          <span>${tile.text}</span>
        </div>
      `).join('');
    }

    function drawRegionChart(regions) {
      const canvas = document.getElementById('regionChart');
      const ctx = canvas.getContext('2d');
      const w = canvas.width;
      const h = canvas.height;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = '#fbfcfd';
      ctx.fillRect(0, 0, w, h);
      const margin = { left: 150, right: 150, top: 96, bottom: 70 };
      const chartW = w - margin.left - margin.right;
      const rowH = (h - margin.top - margin.bottom) / regions.length;
      const maxPotential = Math.max(...regions.map(r => r.potential_clipping));
      ctx.fillStyle = '#1d2733';
      ctx.font = '800 22px Arial';
      ctx.fillText('区域处理表现：潜在削峰、成功处理与失败压力', 24, 36);
      ctx.fillStyle = '#647383';
      ctx.font = '13px Arial';
      ctx.fillText('横条为潜在削峰站点规模，绿色为成功处理，红色为失败/跳过，右侧圆环为成功率。', 24, 64);
      ctx.fillText('成功处理', w - 440, 64);
      ctx.fillStyle = '#138a55';
      fillRoundRect(ctx, w - 372, 55, 20, 10, 5, '#138a55');
      ctx.fillStyle = '#647383';
      ctx.fillText('失败/跳过', w - 330, 64);
      ctx.fillStyle = '#c93636';
      fillRoundRect(ctx, w - 252, 55, 20, 10, 5, '#c93636');
      ctx.fillStyle = '#647383';
      ctx.fillText('成功率', w - 210, 64);
      ctx.strokeStyle = '#e2e8f0';
      ctx.lineWidth = 1;
      ctx.setLineDash([4, 5]);
      for (let i = 0; i <= 4; i += 1) {
        const x = margin.left + chartW * i / 4;
        ctx.beginPath();
        ctx.moveTo(x, margin.top - 6);
        ctx.lineTo(x, h - margin.bottom + 8);
        ctx.stroke();
        ctx.fillStyle = '#8a97a8';
        ctx.fillText(`${Math.round(maxPotential * i / 4 / 1000)}k`, x - 7, h - 28);
      }
      ctx.setLineDash([]);
      regions.forEach((r, i) => {
        const y = margin.top + i * rowH + 9;
        const barH = 21;
        const barW = Math.max(2, chartW * r.potential_clipping / maxPotential);
        const successW = barW * (r.success / Math.max(1, r.potential_clipping));
        const failW = Math.max(0, barW - successW);
        ctx.fillStyle = '#445466';
        ctx.font = '700 12px Arial';
        ctx.fillText(r.area.replace('Europe/', ''), 14, y + 15);
        fillRoundRect(ctx, margin.left, y, chartW, barH, 11, '#edf2f7');
        if (successW > 0) fillRoundRect(ctx, margin.left, y, successW, barH, 11, '#138a55');
        if (failW > 0) fillRoundRect(ctx, margin.left + successW, y, failW, barH, 11, '#c93636');
        ctx.fillStyle = '#1d2733';
        ctx.font = '12px Arial';
        ctx.fillText(fmt.format(r.potential_clipping), margin.left + Math.min(barW + 8, chartW - 52), y + 15);
        const rateColor = r.success_rate < 35 ? '#c93636' : r.success_rate < 50 ? '#b46904' : '#138a55';
        drawRing(ctx, margin.left + chartW + 58, y + 10, 22, r.success_rate, rateColor, `${Math.round(r.success_rate)}%`);
      });
      strokeRoundRect(ctx, margin.left, margin.top - 6, chartW, h - margin.top - margin.bottom + 14, 8, '#d9e0e8');
    }

    function drawIssueChart(issues) {
      const canvas = document.getElementById('issueChart');
      const ctx = canvas.getContext('2d');
      const w = canvas.width;
      const h = canvas.height;
      const rows = issues.slice(0, 7);
      const margin = { left: 84, right: 96, top: 102, bottom: 128 };
      const chartW = w - margin.left - margin.right;
      const chartH = h - margin.top - margin.bottom;
      const maxCount = Math.max(...rows.map(item => item.count));
      const total = rows.reduce((sum, item) => sum + item.count, 0);
      const colors = ['#c93636', '#b46904', '#087f8c', '#2f6fed', '#647383', '#8a5a00', '#7a3f3f'];
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = '#fbfcfd';
      ctx.fillRect(0, 0, w, h);
      ctx.fillStyle = '#1d2733';
      ctx.font = '800 22px Arial';
      ctx.fillText('问题归因 Pareto：失败原因与累计影响', 24, 36);
      ctx.font = '13px Arial';
      ctx.fillStyle = '#647383';
      ctx.fillText('柱状图为日志次数，折线为累计占比；超过 80% 的问题桶优先进入数据质量与工程契约治理。', 24, 64);
      ctx.fillText('日志次数', w - 410, 64);
      fillRoundRect(ctx, w - 342, 55, 20, 10, 5, '#c93636');
      ctx.fillStyle = '#647383';
      ctx.fillText('累计占比', w - 300, 64);
      ctx.strokeStyle = '#087f8c';
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.moveTo(w - 230, 60);
      ctx.lineTo(w - 184, 60);
      ctx.stroke();
      ctx.strokeStyle = '#e2e8f0';
      ctx.lineWidth = 1;
      for (let i = 0; i <= 4; i += 1) {
        const y = margin.top + chartH * i / 4;
        ctx.beginPath();
        ctx.moveTo(margin.left, y);
        ctx.lineTo(margin.left + chartW, y);
        ctx.stroke();
        ctx.fillStyle = '#8a97a8';
        const label = fmt.format(Math.round(maxCount * (1 - i / 4)));
        ctx.fillText(label, 24, y + 4);
      }
      const y80 = margin.top + chartH * 0.2;
      ctx.setLineDash([6, 5]);
      ctx.strokeStyle = '#b46904';
      ctx.beginPath();
      ctx.moveTo(margin.left, y80);
      ctx.lineTo(margin.left + chartW, y80);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = '#b46904';
      ctx.fillText('80%', margin.left + chartW + 12, y80 + 4);

      const colW = chartW / rows.length;
      let cumulative = 0;
      const points = [];
      rows.forEach((item, idx) => {
        cumulative += item.count;
        const x = margin.left + idx * colW + colW * 0.18;
        const barW = colW * 0.64;
        const barH = Math.max(4, chartH * item.count / maxCount);
        const y = margin.top + chartH - barH;
        fillRoundRect(ctx, x, y, barW, barH, 7, colors[idx % colors.length]);
        ctx.fillStyle = '#1d2733';
        ctx.font = '700 12px Arial';
        ctx.textAlign = 'center';
        ctx.fillText(fmt.format(item.count), x + barW / 2, y - 7);
        ctx.save();
        ctx.translate(x + barW / 2 - 2, margin.top + chartH + 34);
        ctx.rotate(-Math.PI / 6);
        ctx.fillStyle = '#425466';
        ctx.font = '13px Arial';
        ctx.fillText(shortText(item.type, 10), 0, 0);
        ctx.restore();
        const paretoY = margin.top + chartH * (1 - cumulative / Math.max(1, total));
        points.push([x + barW / 2, paretoY, cumulative / Math.max(1, total) * 100]);
      });
      ctx.strokeStyle = '#087f8c';
      ctx.lineWidth = 3;
      ctx.beginPath();
      points.forEach(([x, y], idx) => idx === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y));
      ctx.stroke();
      points.forEach(([x, y, value], idx) => {
        ctx.fillStyle = '#087f8c';
        ctx.beginPath();
        ctx.arc(x, y, 5, 0, Math.PI * 2);
        ctx.fill();
        const prevValue = idx > 0 ? points[idx - 1][2] : 0;
        if (idx === 0 || idx === points.length - 1 || value >= 80 && prevValue < 80) {
          ctx.fillStyle = '#087f8c';
          ctx.font = '700 11px Arial';
          ctx.textAlign = 'center';
          ctx.fillText(`${Math.round(value)}%`, x, y - 12);
        }
      });
      ctx.textAlign = 'left';
      ctx.lineWidth = 1;
      strokeRoundRect(ctx, margin.left, margin.top, chartW, chartH, 8, '#d9e0e8');
    }

    function drawEmptyCurve(message) {
      const canvas = document.getElementById('restorationCurveChart');
      const ctx = canvas.getContext('2d');
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.fillStyle = '#fbfcfd';
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      ctx.fillStyle = '#647383';
      ctx.font = '14px Arial';
      ctx.fillText(message || '选择站点和日期后展示还原前后曲线', 28, 42);
      strokeRoundRect(ctx, 18, 18, canvas.width - 36, canvas.height - 36, 8, '#d9e0e8');
    }

    function drawRestorationCurve(curve) {
      if (!curve || !curve.points || !curve.points.length) {
        drawEmptyCurve('该站点当天没有可展示的还原曲线。');
        return;
      }
      const canvas = document.getElementById('restorationCurveChart');
      const ctx = canvas.getContext('2d');
      const w = canvas.width;
      const h = canvas.height;
      const points = curve.points;
      const margin = { left: 58, right: 28, top: 58, bottom: 48 };
      const chartW = w - margin.left - margin.right;
      const chartH = h - margin.top - margin.bottom;
      const values = [];
      points.forEach(p => {
        if (num(p.observed_power) != null) values.push(num(p.observed_power));
        if (num(p.restored_power) != null) values.push(num(p.restored_power));
      });
      const maxY = Math.max(0.1, Math.max.apply(null, values) * 1.16);
      const xFor = idx => margin.left + chartW * idx / Math.max(1, points.length - 1);
      const yFor = value => margin.top + chartH - clamp((value || 0) / maxY, 0, 1) * chartH;

      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = '#fbfcfd';
      ctx.fillRect(0, 0, w, h);
      ctx.fillStyle = '#1d2733';
      ctx.font = '700 15px Arial';
      ctx.fillText(`线上还原曲线 · ${modelStageName(curve.model_name)} · ${curve.model_name || 'model'}`, 18, 30);
      const stats = curve.stats || {};
      const scaleLabel = `还原预测值固定 x${Number(stats.scale_factor || 12).toFixed(0)} 展示，多发电量按 5 分钟积分`;
      ctx.fillStyle = '#647383';
      ctx.font = '12px Arial';
      ctx.fillText(scaleLabel, margin.left, 30);

      ctx.strokeStyle = '#e2e8f0';
      ctx.lineWidth = 1;
      for (let i = 0; i <= 4; i += 1) {
        const y = margin.top + chartH * i / 4;
        ctx.beginPath();
        ctx.moveTo(margin.left, y);
        ctx.lineTo(margin.left + chartW, y);
        ctx.stroke();
        ctx.fillStyle = '#8a97a8';
        ctx.fillText((maxY * (1 - i / 4)).toFixed(2), 14, y + 4);
      }
      for (let hour = 0; hour <= 24; hour += 6) {
        const idx = Math.min(points.length - 1, Math.round(hour * 12));
        const x = xFor(idx);
        ctx.strokeStyle = '#eef2f6';
        ctx.beginPath();
        ctx.moveTo(x, margin.top);
        ctx.lineTo(x, margin.top + chartH);
        ctx.stroke();
        ctx.fillStyle = '#8a97a8';
        ctx.fillText(`${hour.toString().padStart(2, '0')}:00`, x - 16, h - 18);
      }

      const observed = points.map(p => num(p.observed_power));
      const restored = points.map(p => num(p.restored_power));
      ctx.fillStyle = 'rgba(19, 138, 85, .16)';
      for (let idx = 0; idx < points.length - 1; idx += 1) {
        const before0 = observed[idx];
        const after0 = restored[idx];
        const before1 = observed[idx + 1];
        const after1 = restored[idx + 1];
        if (before0 == null || after0 == null || before1 == null || after1 == null) continue;
        if (after0 <= before0 && after1 <= before1) continue;
        ctx.beginPath();
        ctx.moveTo(xFor(idx), yFor(Math.max(after0, before0)));
        ctx.lineTo(xFor(idx + 1), yFor(Math.max(after1, before1)));
        ctx.lineTo(xFor(idx + 1), yFor(before1));
        ctx.lineTo(xFor(idx), yFor(before0));
        ctx.closePath();
        ctx.fill();
      }

      function drawLine(values, color, width) {
        ctx.strokeStyle = color;
        ctx.lineWidth = width;
        ctx.beginPath();
        let active = false;
        values.forEach((value, idx) => {
          if (value == null) {
            active = false;
            return;
          }
          const x = xFor(idx);
          const y = yFor(value);
          if (!active) {
            ctx.moveTo(x, y);
            active = true;
          } else {
            ctx.lineTo(x, y);
          }
        });
        ctx.stroke();
      }
      drawLine(observed, '#b46904', 2);
      drawLine(restored, '#138a55', 3);

      const peakIdx = restored.reduce((best, value, idx) => {
        if (value == null) return best;
        return best < 0 || value > restored[best] ? idx : best;
      }, -1);
      if (peakIdx >= 0) {
        ctx.fillStyle = '#138a55';
        ctx.beginPath();
        ctx.arc(xFor(peakIdx), yFor(restored[peakIdx]), 5, 0, Math.PI * 2);
        ctx.fill();
        ctx.fillStyle = '#1d2733';
        ctx.font = '700 12px Arial';
        ctx.fillText(`峰值 ${fmtPower(restored[peakIdx])}`, xFor(peakIdx) + 8, yFor(restored[peakIdx]) - 8);
      }
      ctx.lineWidth = 1;
      strokeRoundRect(ctx, margin.left, margin.top, chartW, chartH, 8, '#d9e0e8');
    }

    function drawEmptyComparison(message) {
      const canvas = document.getElementById('modelEnergyCompareChart');
      const ctx = canvas.getContext('2d');
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.fillStyle = '#fbfcfd';
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      ctx.fillStyle = '#647383';
      ctx.font = '14px Arial';
      ctx.fillText(message || '查询站点后展示第一版与迭代版多发电量对比', 28, 42);
      strokeRoundRect(ctx, 18, 18, canvas.width - 36, canvas.height - 36, 8, '#d9e0e8');
    }

    function drawModelEnergyCompare(data) {
      const curves = data && data.curves ? data.curves : [];
      const compare = comparisonCurves(curves);
      if (!compare.rows.length) {
        drawEmptyComparison('未读取到可对比模型。');
        return;
      }
      const canvas = document.getElementById('modelEnergyCompareChart');
      const ctx = canvas.getContext('2d');
      const w = canvas.width;
      const h = canvas.height;
      const baseline = compare.baseline;
      const iteration = compare.iteration;
      const reference = iteration || baseline || compare.rows[0].curve;
      const points = reference.points || [];
      const main = {
        x: 92,
        y: 126,
        w: Math.min(1120, w - 560),
        h: h - 250
      };
      const side = {
        x: main.x + main.w + 58,
        y: main.y,
        w: w - main.x - main.w - 128,
        h: main.h
      };
      const observed = points.map(p => num(p.observed_power));
      const baselineValues = baseline && baseline.points ? baseline.points.map(p => num(p.restored_power)) : [];
      const iterationValues = iteration && iteration.points ? iteration.points.map(p => num(p.restored_power)) : [];
      const allValues = [...observed, ...baselineValues, ...iterationValues].filter(v => v != null);
      const maxY = Math.max(0.1, Math.max(...allValues) * 1.18);
      const xForPoint = idx => main.x + main.w * idx / Math.max(1, points.length - 1);
      const yForPower = value => main.y + main.h - clamp((value || 0) / maxY, 0, 1) * main.h;
      const energyRows = compare.rows.slice(0, 2);
      const maxEnergy = Math.max(1, ...energyRows.map(row => extraEnergyOf(row.curve))) * 1.18;

      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = '#fbfcfd';
      ctx.fillRect(0, 0, w, h);
      ctx.fillStyle = '#1d2733';
      ctx.font = '800 22px Arial';
      ctx.fillText('双模型迭代效果检验：同一站点、同一日期、同一输入', 24, 36);
      ctx.fillStyle = '#647383';
      ctx.font = '13px Arial';
      ctx.fillText('左侧同图叠加还原前观测、Restoration 第一版、10000station 迭代版；右侧按多发电量 kWh 做灰度判断。', 24, 64);
      const legendStart = Math.max(760, Math.min(w - 540, main.x + main.w - 300));
      const legend = [
        { x: legendStart, label: '观测 PV', color: '#b46904' },
        { x: legendStart + 116, label: '第一版', color: '#2f6fed' },
        { x: legendStart + 220, label: '迭代版', color: '#087f8c' }
      ];
      legend.forEach(item => {
        ctx.strokeStyle = item.color;
        ctx.lineWidth = 4;
        ctx.beginPath();
        ctx.moveTo(item.x, 60);
        ctx.lineTo(item.x + 32, 60);
        ctx.stroke();
        ctx.fillStyle = '#647383';
        ctx.font = '13px Arial';
        ctx.fillText(item.label, item.x + 40, 64);
      });

      ctx.strokeStyle = '#e2e8f0';
      ctx.lineWidth = 1;
      for (let i = 0; i <= 4; i += 1) {
        const y = main.y + main.h * i / 4;
        ctx.beginPath();
        ctx.moveTo(main.x, y);
        ctx.lineTo(main.x + main.w, y);
        ctx.stroke();
        ctx.fillStyle = '#8a97a8';
        ctx.fillText(`${(maxY * (1 - i / 4)).toFixed(1)}`, 28, y + 4);
      }
      for (let hour = 0; hour <= 24; hour += 6) {
        const idx = Math.min(points.length - 1, Math.round(hour * 12));
        const x = xForPoint(idx);
        ctx.strokeStyle = '#eef2f6';
        ctx.beginPath();
        ctx.moveTo(x, main.y);
        ctx.lineTo(x, main.y + main.h);
        ctx.stroke();
        ctx.fillStyle = '#8a97a8';
        ctx.font = '12px Arial';
        ctx.fillText(`${hour.toString().padStart(2, '0')}:00`, x - 16, h - 32);
      }

      if (baseline && iteration && baselineValues.length && iterationValues.length) {
        ctx.fillStyle = 'rgba(8, 127, 140, .10)';
        for (let idx = 0; idx < Math.min(baselineValues.length, iterationValues.length) - 1; idx += 1) {
          const b0 = baselineValues[idx];
          const i0 = iterationValues[idx];
          const b1 = baselineValues[idx + 1];
          const i1 = iterationValues[idx + 1];
          if (b0 == null || i0 == null || b1 == null || i1 == null) continue;
          ctx.beginPath();
          ctx.moveTo(xForPoint(idx), yForPower(b0));
          ctx.lineTo(xForPoint(idx + 1), yForPower(b1));
          ctx.lineTo(xForPoint(idx + 1), yForPower(i1));
          ctx.lineTo(xForPoint(idx), yForPower(i0));
          ctx.closePath();
          ctx.fill();
        }
      }

      function drawSeries(values, color, width, dashed = false) {
        ctx.strokeStyle = color;
        ctx.lineWidth = width;
        ctx.setLineDash(dashed ? [8, 7] : []);
        ctx.beginPath();
        let active = false;
        values.forEach((value, idx) => {
          if (value == null) {
            active = false;
            return;
          }
          const x = xForPoint(idx);
          const y = yForPower(value);
          if (!active) {
            ctx.moveTo(x, y);
            active = true;
          } else {
            ctx.lineTo(x, y);
          }
        });
        ctx.stroke();
        ctx.setLineDash([]);
      }
      drawSeries(observed, '#b46904', 2.5, true);
      if (baselineValues.length) drawSeries(baselineValues, '#2f6fed', 3.5);
      if (iterationValues.length) drawSeries(iterationValues, '#087f8c', 3.5);

      ctx.strokeStyle = '#d9e0e8';
      strokeRoundRect(ctx, main.x, main.y, main.w, main.h, 8, '#d9e0e8');

      fillRoundRect(ctx, side.x, side.y, side.w, side.h, 8, '#ffffff');
      strokeRoundRect(ctx, side.x, side.y, side.w, side.h, 8, '#d9e0e8');
      ctx.fillStyle = '#1d2733';
      ctx.font = '800 17px Arial';
      ctx.fillText('多发电量 kWh', side.x + 18, side.y + 34);
      ctx.fillStyle = '#647383';
      ctx.font = '12px Arial';
      ctx.fillText('正向增益面积积分', side.x + 18, side.y + 55);

      energyRows.forEach((row, idx) => {
        const energy = extraEnergyOf(row.curve);
        const barX = side.x + 18;
        const barY = side.y + 104 + idx * 118;
        const barW = (side.w - 36) * energy / maxEnergy;
        fillRoundRect(ctx, barX, barY, side.w - 36, 18, 9, '#edf2f7');
        fillRoundRect(ctx, barX, barY, Math.max(4, barW), 18, 9, row.color);
        ctx.fillStyle = '#1d2733';
        ctx.font = '800 18px Arial';
        ctx.fillText(`${fmtPower(energy)} kWh`, barX, barY - 12);
        ctx.fillStyle = '#647383';
        ctx.font = '13px Arial';
        ctx.fillText(row.label, barX, barY + 42);
      });

      if (compare.baseline && compare.iteration) {
        const baselineEnergy = extraEnergyOf(compare.baseline);
        const iterationEnergy = extraEnergyOf(compare.iteration);
        const delta = iterationEnergy - baselineEnergy;
        const ratio = baselineEnergy ? delta / baselineEnergy * 100 : 0;
        const color = delta >= 0 ? '#138a55' : '#b46904';
        const pillX = side.x + 18;
        const pillY = side.y + side.h - 92;
        fillRoundRect(ctx, pillX, pillY, side.w - 36, 68, 8, delta >= 0 ? '#eaf7f0' : '#fff5df');
        ctx.fillStyle = color;
        ctx.font = '800 20px Arial';
        ctx.fillText(`${delta >= 0 ? '+' : ''}${fmtPower(delta)} kWh`, pillX + 16, pillY + 30);
        ctx.font = '13px Arial';
        ctx.fillText(`迭代版相对第一版 ${delta >= 0 ? '提升' : '下降'} ${ratio >= 0 ? '+' : ''}${ratio.toFixed(2)}%`, pillX + 16, pillY + 52);
      }

      ctx.lineWidth = 1;
      ctx.textAlign = 'left';
    }

    function renderModelComparison(data) {
      const curves = data && data.curves ? data.curves : [];
      const compare = comparisonCurves(curves);
      const baseline = compare.baseline;
      const iteration = compare.iteration;
      if (!compare.rows.length) {
        document.getElementById('modelCompareCards').innerHTML = '<div class="compare-card"><span>模型对比</span><strong>无数据</strong><small>未读取到可对比模型。</small></div>';
        drawEmptyComparison('未读取到可对比模型。');
        return;
      }
      const baselineEnergy = baseline ? extraEnergyOf(baseline) : null;
      const iterationEnergy = iteration ? extraEnergyOf(iteration) : null;
      const delta = baselineEnergy != null && iterationEnergy != null ? iterationEnergy - baselineEnergy : null;
      const ratio = delta != null && baselineEnergy ? delta / baselineEnergy * 100 : null;
      const winner = baseline && iteration
        ? (iterationEnergy >= baselineEnergy ? '迭代版本日收益更高' : '本日样例第一版更高')
        : '模型不足，无法两版对比';
      const conclusion = baseline && iteration
        ? (iterationEnergy >= baselineEnergy ? '候选模型在该站该日表现占优，可继续扩大灰度。' : '候选模型在该站该日偏保守，应回流样本继续分析。')
        : '需要两版模型同时存在后再做晋升判断。';
      document.getElementById('modelCompareCards').innerHTML = [
        `<div class="compare-card"><span>Restoration 第一版</span><strong>${baseline ? `${fmtPower(baselineEnergy)} kWh` : '-'}</strong><small>${baseline ? baseline.model_name : '未命中第一版模型'}</small></div>`,
        `<div class="compare-card iteration"><span>10000station 迭代版</span><strong>${iteration ? `${fmtPower(iterationEnergy)} kWh` : '-'}</strong><small>${iteration ? iteration.model_name : '未命中迭代版模型'}</small></div>`,
        `<div class="compare-card ${delta != null && delta >= 0 ? 'delta' : 'warn'}"><span>迭代净提升</span><strong>${delta != null ? `${delta >= 0 ? '+' : ''}${fmtPower(delta)} kWh` : '-'}</strong><small>${ratio != null ? `${ratio >= 0 ? '+' : ''}${ratio.toFixed(2)}% vs 第一版` : '需要两版模型同时存在'}</small></div>`,
        `<div class="compare-card ${delta != null && delta >= 0 ? 'delta' : 'warn'}"><span>对比结论</span><strong>${winner}</strong><small>${conclusion}</small></div>`
      ].join('');
      drawModelEnergyCompare(data);
    }

    function renderRestorationCurve(data, selectedIndex = 0) {
      lastRestorationData = data;
      const curves = data && data.curves ? data.curves : [];
      const curve = curves[selectedIndex] || data && data.selected_curve;
      if (!curve) {
        document.getElementById('curveCards').innerHTML = '<div class="curve-card"><span>曲线状态</span><strong>无数据</strong><small>未读取到还原结果或原始功率。</small></div>';
        document.getElementById('curveModels').innerHTML = '';
        document.getElementById('modelCompareCards').innerHTML = '<div class="compare-card"><span>模型对比</span><strong>无数据</strong><small>未读取到可对比模型。</small></div>';
        drawEmptyCurve('未读取到该站点当天的还原前后曲线。');
        drawEmptyComparison('未读取到可对比模型。');
        return;
      }
      const stats = curve.stats || {};
      const extraEnergy = stats.extra_generation_energy != null ? stats.extra_generation_energy : stats.restored_gain_energy;
      const scaleText = `还原预测值固定 x${Number(stats.scale_factor || 12).toFixed(0)} 展示`;
      document.getElementById('curveCards').innerHTML = [
        `<div class="curve-card"><span>观测峰值</span><strong>${fmtPower(stats.observed_peak)}</strong><small>还原前 filtered_pv_total_power</small></div>`,
        `<div class="curve-card"><span>还原峰值</span><strong>${fmtPower(stats.restored_peak)}</strong><small>${curve.model_name || ''}</small></div>`,
        `<div class="curve-card"><span>多发电量</span><strong>${fmtPower(extraEnergy)} kWh</strong><small>还原高于观测部分积分，约 ${fmtPower(stats.gain_ratio)}%</small></div>`,
        `<div class="curve-card"><span>恢复点数</span><strong>${fmt.format(stats.gain_points || 0)}</strong><small>${fmt.format(curve.point_count || 0)} 点；${scaleText}</small></div>`
      ].join('');
      document.getElementById('curveModels').innerHTML = curves.map((item, idx) => `
        <button class="model-chip ${idx === selectedIndex ? 'active' : ''}" type="button" data-curve-index="${idx}">
          ${modelDisplayName(item.model_name)} · ${item.record_time || '-'}
        </button>
      `).join('');
      document.querySelectorAll('[data-curve-index]').forEach(btn => {
        btn.addEventListener('click', () => renderRestorationCurve(lastRestorationData, Number(btn.dataset.curveIndex)));
      });
      drawRestorationCurve(curve);
      renderModelComparison(data);
    }

    function clearRestorationCurve(message) {
      lastRestorationData = null;
      document.getElementById('curveModels').innerHTML = '';
      document.getElementById('curveCards').innerHTML = `<div class="curve-card"><span>曲线状态</span><strong>待查询</strong><small>${message || '选择站点后自动读取原始功率和还原结果。'}</small></div>`;
      document.getElementById('modelCompareCards').innerHTML = `<div class="compare-card"><span>模型对比</span><strong>待查询</strong><small>${message || '查询站点后展示第一版与迭代版多发电量。'}</small></div>`;
      drawEmptyCurve(message || '选择站点和日期后展示还原前后曲线');
      drawEmptyComparison(message || '查询站点后展示第一版与迭代版多发电量对比');
    }

    function openChartZoom(canvasId, title) {
      const canvas = document.getElementById(canvasId);
      const overlay = document.getElementById('chartZoomOverlay');
      const image = document.getElementById('zoomImage');
      document.getElementById('zoomTitle').textContent = title || '图表放大';
      image.src = canvas.toDataURL('image/png');
      overlay.classList.add('active');
      overlay.setAttribute('aria-hidden', 'false');
    }

    function closeChartZoom() {
      const overlay = document.getElementById('chartZoomOverlay');
      overlay.classList.remove('active');
      overlay.setAttribute('aria-hidden', 'true');
      document.getElementById('zoomImage').removeAttribute('src');
    }

    function renderTables(regions) {
      document.getElementById('regionRows').innerHTML = regions.map(r => `
        <tr>
          <td>${r.area}</td>
          <td class="num">${fmt.format(r.total_stations)}</td>
          <td class="num">${fmt.format(r.potential_clipping)}</td>
          <td class="num">${pct(r.success_rate)}</td>
          <td class="num">${fmt.format(r.failed_or_skipped)}</td>
        </tr>
      `).join('');
    }

    function stateLabel(state) {
      const labels = {
        running: '已上线',
        gray: '灰度中',
        planned_partial: '规划/部分实现',
        implemented: '已实现'
      };
      return labels[state] || state;
    }

    function renderEvolutionImplementation(data) {
      const feedback = data.sample_feedback || {};
      const dataset = data.dataset_reconstruction || {};
      const buckets = feedback.buckets || [];
      const splits = dataset.splits || {};
      document.getElementById('feedbackStatus').textContent = feedback.status === 'implemented' ? '已实现 · 问题样本驱动迭代' : '待实现';
      document.getElementById('datasetStatus').textContent = dataset.status === 'implemented' ? `${dataset.dataset_version || ''}` : '待实现';
      document.getElementById('feedbackCards').innerHTML = [
        `<div class="feedback-card"><span>样本池版本</span><strong>${feedback.sample_pool_version || '-'}</strong><small>${feedback.date || ''} 日终归因生成</small></div>`,
        `<div class="feedback-card good"><span>回流问题记录</span><strong>${fmt.format(feedback.total_feedback_records || 0)}</strong><small>来自 WARNING / ERROR 自动归因桶</small></div>`,
        `<div class="feedback-card good"><span>可训练样本</span><strong>${fmt.format(feedback.trainable_records || 0)}</strong><small>进入训练、缺失特征训练或新场景训练</small></div>`,
        `<div class="feedback-card danger"><span>契约阻断</span><strong>${fmt.format(feedback.contract_blocked_records || 0)}</strong><small>工程问题不污染训练集</small></div>`
      ].join('');
      document.getElementById('feedbackBuckets').innerHTML = buckets.slice(0, 6).map(row => `
        <div class="feedback-row">
          <div><strong>${row.issue_type}</strong><span>${row.sample_pool_id}</span></div>
          <div><span>${row.scene_label}</span><span>${row.action}</span></div>
          <b>${fmt.format(row.selected_count)}</b>
        </div>
      `).join('');

      const splitRows = [
        ['train', '训练集', splits.train || 0, ''],
        ['validation', '验证集', splits.validation || 0, 'validation'],
        ['gray_eval', '灰度评估集', splits.gray_eval || 0, 'gray'],
        ['quality_holdout', '质量留存', splits.quality_holdout || 0, 'holdout'],
        ['contract_blocklist', '契约阻断', splits.contract_blocklist || 0, 'blocked']
      ];
      const maxSplit = Math.max(1, ...splitRows.map(row => row[2]));
      document.getElementById('datasetCards').innerHTML = [
        `<div class="feedback-card"><span>数据集版本</span><strong>${dataset.dataset_version || '-'}</strong><small>来源 ${dataset.source_sample_pool_version || '-'}</small></div>`,
        `<div class="feedback-card good"><span>可训练记录</span><strong>${fmt.format(dataset.trainable_records || 0)}</strong><small>按 70/15/15 重构训练、验证、灰度集</small></div>`,
        `<div class="feedback-card warn"><span>场景标签</span><strong>${fmt.format((dataset.scene_labels || []).length)}</strong><small>质量、缺失、边界、契约统一编码</small></div>`,
        `<div class="feedback-card good"><span>目标模型</span><strong>${dataset.next_model_target || '-'}</strong><small>驱动下一轮候选模型训练</small></div>`
      ].join('');
      document.getElementById('datasetSplits').innerHTML = splitRows.map(([key, label, value, cls]) => `
        <div class="split-bar">
          <span>${label}</span>
          <div class="split-track"><div class="split-fill ${cls}" style="width:${Math.max(3, value / maxSplit * 100)}%"></div></div>
          <b>${fmt.format(value)}</b>
        </div>
      `).join('');
      document.getElementById('sceneLabels').innerHTML = (dataset.scene_labels || []).slice(0, 5).map(row => `
        <div class="feedback-row">
          <div><strong>#${row.label_id} ${row.scene_label}</strong><span>${row.source_bucket}</span></div>
          <div><span>${row.target_dataset}</span><span>${row.train_policy}</span></div>
          <b>${fmt.format(row.sample_count)}</b>
        </div>
      `).join('');
    }

    function renderLists(data) {
      document.getElementById('alerts').innerHTML = data.alerts.map(a => `
        <div class="alert"><span class="badge ${a.level}">${a.level}</span><strong>${a.title}</strong><p>${a.detail}</p></div>
      `).join('');
      document.getElementById('issues').innerHTML = data.issues.slice(0, 5).map(i => `
        <div class="issue"><strong>${i.type} · ${fmt.format(i.count)} 次</strong><p>${i.meaning}</p><p>${i.action}</p></div>
      `).join('');
      document.getElementById('models').innerHTML = Object.values(data.models).map(m => `
        <div class="model"><strong>${m.name}</strong><p>${m.stage}</p><p>${m.s3_key}</p></div>
      `).join('');
      document.getElementById('steps').innerHTML = data.evolution_steps.map(s => `
        <div class="step"><strong>${s.name}</strong><p><span class="badge ${s.state === 'running' ? 'info' : s.state === 'gray' ? 'warning' : ''}">${stateLabel(s.state)}</span></p></div>
      `).join('');
      document.getElementById('quickQuestions').innerHTML = data.suggested_questions.map(q => `
        <button class="btn" type="button" data-question="${q}">${q}</button>
      `).join('');
      document.querySelectorAll('[data-question]').forEach(btn => {
        btn.addEventListener('click', () => {
          document.getElementById('question').value = btn.dataset.question;
          askAgent(btn.dataset.question);
        });
      });
    }

    function addMessage(role, text) {
      const box = document.getElementById('messages');
      const div = document.createElement('div');
      div.className = `msg ${role}`;
      div.textContent = text;
      box.appendChild(div);
      box.scrollTop = box.scrollHeight;
    }

    function appendEvent(event) {
      const box = document.getElementById('eventLog');
      const line = `[${new Date().toLocaleTimeString()}] ${JSON.stringify(event, null, 2)}`;
      box.textContent = box.textContent === '等待 WebSocket 事件...' ? line : `${line}\n\n${box.textContent}`;
    }

    function renderConfirmTask(task) {
      currentTask = task;
      if (!task) return;
      const answered = task.status === 'answered';
      const choice = task.choice === 'yes' ? 'Yes / 确认' : task.choice === 'no' ? 'No / 拒绝' : '等待选择';
      document.getElementById('confirmCard').innerHTML = `
        <h3>${task.title}</h3>
        <p><strong>任务 ID：</strong>${task.task_id}</p>
        <p><strong>问题：</strong>${task.question}</p>
        <p><strong>状态：</strong>${answered ? `已完成，用户选择 ${choice}` : '等待用户在飞书卡片上点击 Yes 或 No'}</p>
        <p><strong>上下文压缩：</strong>${answered ? JSON.stringify({ task: task.title, card_status: task.status, user_choice: task.choice, next_step: task.choice === 'yes' ? 'continue_execution' : 'stop_or_escalate' }) : 'pending'}</p>
      `;
    }

    function renderOnlineResult(data) {
      const summary = data.summary || {};
      const stationHistory = data.station_history || {};
      const modelLines = (summary.model_breakdown || []).slice(0, 6).map(row =>
        `- ${row.model_name || '(empty)'}: ${fmt.format(row.restored_station_count || 0)} stations, ${fmt.format(row.row_count || 0)} rows`
      ).join('\n');
      const stationRows = (stationHistory.rows || []).slice(0, 5).map((row, idx) =>
        `${idx + 1}. station=${row.station_id}, model=${row.model_name}, record_time=${row.record_time}`
      ).join('\n');
      const stationTitle = stationHistory.station_id
        ? `站点 ${stationHistory.station_id} 明细前 ${stationHistory.row_count || 0} 行：`
        : '未指定站点，当前只展示该日期全站统计。';
      const text = [
        `线上查询：${summary.source || data.source || 'starrocks_9030'}`,
        `日期 dt：${summary.date || ''}`,
        `predict_type：4`,
        `削峰还原入库站点数 COUNT(DISTINCT station_id)：${fmt.format(summary.restored_station_count || 0)}`,
        `记录行数 COUNT(*)：${fmt.format(summary.row_count || 0)}`,
        `模型数：${fmt.format(summary.model_count || 0)}`,
        `record_time 范围：${summary.first_record_time || '-'} -> ${summary.last_record_time || '-'}`,
        '',
        '模型拆分：',
        modelLines || '- 无数据',
        '',
        stationTitle,
        stationRows || '- 无数据',
        '',
        'SQL 口径：SELECT COUNT(DISTINCT station_id) FROM sigen_device.pv_prediction_history WHERE dt = ? AND predict_type = 4'
      ].join('\n');
      document.getElementById('onlineResult').textContent = text;
      document.getElementById('onlineStatus').textContent = data.ok ? '线上查询成功' : '线上查询失败';
    }

    function renderK8sResult(data) {
      const analysis = data.analysis || {};
      const severity = analysis.severity_counts || {};
      const issueLines = Object.entries(analysis.issue_counts || {}).map(([name, count], idx) =>
        `${idx + 1}. ${name}: ${fmt.format(count)} 次`
      ).join('\n');
      const sourceLines = (data.log_sources || []).slice(0, 12).map(src =>
        `- ${src.ok ? 'OK' : 'FAIL'} pod=${src.pod}, container=${src.container || '(default)'}, lines=${fmt.format(src.line_count || 0)}${src.error ? ', error=' + src.error : ''}`
      ).join('\n');
      const podLines = (data.selected_pods || []).slice(0, 12).map(pod =>
        `- ${pod.name} [${pod.status || '-'}] containers=${(pod.containers || []).join(',') || '-'}`
      ).join('\n');
      const sampleLines = (analysis.samples || []).slice(0, 8).map((line, idx) => `${idx + 1}. ${line}`).join('\n');
      const run = analysis.run_summary || {};
      const text = [
        `K8s Dashboard 日志分析：${data.source || 'k8s_dashboard'}`,
        `namespace：${data.namespace}`,
        `关键词：${(data.keywords || []).join(', ')}`,
        `匹配 Pod：${fmt.format(data.matched_pods || 0)} / ${fmt.format(data.pod_total || 0)}`,
        `读取日志源：${fmt.format((data.log_sources || []).length)}，总行数：${fmt.format(analysis.line_count || 0)}`,
        `级别统计：ERROR=${fmt.format(severity.ERROR || 0)}, WARNING=${fmt.format(severity.WARNING || 0)}, INFO=${fmt.format(severity.INFO || 0)}`,
        `运行汇总：潜在=${run.potential_clipping_stations != null ? run.potential_clipping_stations : '-'}, 成功=${run.success_stations != null ? run.success_stations : '-'}, 失败/跳过=${run.failed_or_skipped_stations != null ? run.failed_or_skipped_stations : '-'}`,
        '',
        '问题归因：',
        issueLines || '- 未识别到已知问题桶',
        '',
        '匹配 Pod：',
        podLines || '- 无',
        '',
        '日志源：',
        sourceLines || '- 无',
        '',
        '错误样例：',
        sampleLines || '- 无'
      ].join('\n');
      document.getElementById('k8sResult').textContent = text;
      document.getElementById('k8sStatus').textContent = data.ok ? 'K8s 日志分析成功' : 'K8s 日志分析失败';
    }

    async function loadOnlineData(options = {}) {
      const date = document.getElementById('onlineDate').value.trim();
      const stationId = options.allStations ? '' : document.getElementById('onlineStation').value.trim();
      const modelName = document.getElementById('onlineModel').value.trim();
      const limit = document.getElementById('onlineLimit').value.trim() || '20';
      if (!date) {
        addMessage('agent', '请先选择要查询的日期 dt。');
        return;
      }
      if (options.allStations) {
        document.getElementById('onlineStation').value = '';
      }
      document.getElementById('onlineStatus').textContent = '正在查询 StarRocks 9030...';
      document.getElementById('onlineResult').textContent = '查询中...';
      try {
        const params = new URLSearchParams({ date, limit });
        if (stationId) params.set('station_id', stationId);
        if (modelName) params.set('model_name', modelName);
        const data = await api(`/api/online/clipping-summary?${params.toString()}`);
        renderOnlineResult(data);
        if (stationId) {
          await loadRestorationCurve(date, stationId, modelName);
        } else {
          clearRestorationCurve('当前为全站统计。输入 station_id 后展示单站还原前后曲线。');
        }
        const stationMsg = stationId ? `；站点 ${stationId} 命中 ${(data.station_history && data.station_history.row_count) || 0} 条记录` : '；本次查询为全站统计';
        addMessage('agent', `线上 9030 查询完成：${date} predict_type=4 下共有 ${fmt.format(data.summary.restored_station_count || 0)} 个去重站点有削峰还原结果${stationMsg}。`);
      } catch (err) {
        document.getElementById('onlineStatus').textContent = '线上查询失败';
        document.getElementById('onlineResult').textContent = err.message;
        addMessage('agent', `线上查询失败：${err.message}`);
      }
    }

    async function loadRestorationCurve(date, stationId, modelName) {
      const params = new URLSearchParams({ date, station_id: stationId });
      if (modelName) params.set('model_name', modelName);
      try {
        const data = await api(`/api/online/restoration-curve?${params.toString()}`);
        renderRestorationCurve(data, 0);
      } catch (err) {
        clearRestorationCurve(`还原曲线读取失败：${err.message}`);
      }
    }

    async function loadK8sLogs() {
      const namespace = document.getElementById('k8sNamespace').value.trim() || 'data-platform';
      const keywords = document.getElementById('k8sKeywords').value.trim() || 'sigen-pv-clipping';
      const tailLines = document.getElementById('k8sTailLines').value.trim() || '500';
      const limitPods = document.getElementById('k8sLimitPods').value.trim() || '8';
      document.getElementById('k8sStatus').textContent = '正在读取 K8s Dashboard 日志...';
      document.getElementById('k8sResult').textContent = '查询中...';
      try {
        const params = new URLSearchParams({
          namespace,
            keywords,
          tail_lines: tailLines,
          limit_pods: limitPods
        });
        const data = await api(`/api/k8s/log-summary?${params.toString()}`);
        renderK8sResult(data);
        const severity = (data.analysis && data.analysis.severity_counts) || {};
        addMessage('agent', `K8s 日志分析完成：匹配 ${fmt.format(data.matched_pods || 0)} 个 Pod，读取 ${fmt.format((data.log_sources || []).length)} 个日志源，ERROR=${fmt.format(severity.ERROR || 0)}，WARNING=${fmt.format(severity.WARNING || 0)}。`);
      } catch (err) {
        document.getElementById('k8sStatus').textContent = 'K8s 日志分析失败';
        document.getElementById('k8sResult').textContent = err.message;
        addMessage('agent', `K8s 日志分析失败：${err.message}`);
      }
    }

    function connectEvents() {
      const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
      const ws = new WebSocket(`${protocol}//${location.host}/ws/events`);
      ws.onopen = () => {
        document.getElementById('wsStatus').textContent = 'WebSocket 已连接';
      };
      ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        appendEvent(data);
        const payload = data && data.payload ? data.payload : {};
        const result = payload && payload.result ? payload.result : {};
        const task = payload.task || payload.task_state || result.task;
        if (task && task.task_id) renderConfirmTask(task);
      };
      ws.onclose = () => {
        document.getElementById('wsStatus').textContent = 'WebSocket 已断开，3 秒后重连';
        setTimeout(connectEvents, 3000);
      };
      ws.onerror = () => {
        document.getElementById('wsStatus').textContent = 'WebSocket 连接异常';
      };
    }

    async function askAgent(text) {
      const question = (text || document.getElementById('question').value).trim();
      if (!question) return;
      addMessage('user', question);
      document.getElementById('question').value = '';
      try {
        const data = await api('/api/ask', { method: 'POST', body: JSON.stringify({ question }) });
        addMessage('agent', data.answer);
      } catch (err) {
        addMessage('agent', `请求失败：${err.message}`);
      }
    }

    async function refresh() {
      snapshot = await api('/api/status');
      document.getElementById('generatedAt').textContent = `生成时间 ${snapshot.generated_at}`;
      renderMetrics(snapshot.summary);
      renderTables(snapshot.regions);
      renderLists(snapshot);
      renderEvolutionImplementation(snapshot);
      renderPipeline(snapshot.evolution_steps);
      renderModelCompare(snapshot.models);
      renderRegionInsights(snapshot.regions);
      renderIssueInsights(snapshot.issues);
      drawRegionChart(snapshot.regions);
      drawIssueChart(snapshot.issues);
      if (!lastRestorationData) {
        clearRestorationCurve('选择站点和日期后展示还原前后曲线。');
      }
      if (snapshot.card_tasks && snapshot.card_tasks.length) {
        renderConfirmTask(snapshot.card_tasks[snapshot.card_tasks.length - 1]);
      }
      if (!document.getElementById('messages').children.length) {
        addMessage('agent', '我已加载光擎 SolarRestore Agent 看板。你可以问运行状态、失败归因、区域风险、新旧模型灰度、样本回流、K8s 容器日志，或直接问：线上9030 2026-05-22 predict_type=4 多少个站？');
      }
    }

    document.getElementById('chatForm').addEventListener('submit', (event) => {
      event.preventDefault();
      askAgent();
    });
    document.getElementById('refreshBtn').addEventListener('click', refresh);
    document.getElementById('simulateBtn').addEventListener('click', async () => {
      const data = await api('/api/simulate-run', { method: 'POST', body: JSON.stringify({ area: 'Europe/*' }) });
      document.getElementById('reportText').textContent = data.report;
      document.getElementById('reportStatus').textContent = `已生成 ${new Date().toLocaleTimeString()}`;
    });
    document.getElementById('onlineBtn').addEventListener('click', loadOnlineData);
    document.getElementById('loadOnlineBtn').addEventListener('click', loadOnlineData);
    document.getElementById('queryAllOnlineBtn').addEventListener('click', () => loadOnlineData({ allStations: true }));
    document.getElementById('k8sBtn').addEventListener('click', loadK8sLogs);
    document.getElementById('loadK8sBtn').addEventListener('click', loadK8sLogs);
    document.getElementById('k8sClippingBtn').addEventListener('click', () => {
      document.getElementById('k8sNamespace').value = 'data-platform';
      document.getElementById('k8sKeywords').value = 'sigen-pv-clipping';
      loadK8sLogs();
    });
    document.getElementById('k8sDataPlatformBtn').addEventListener('click', () => {
      document.getElementById('k8sNamespace').value = 'data-platform';
      document.getElementById('k8sKeywords').value = 'sigen-pv-clipping';
      loadK8sLogs();
    });
    document.getElementById('sampleOnlineBtn').addEventListener('click', () => {
      document.getElementById('onlineDate').value = '2026-05-22';
      document.getElementById('onlineStation').value = '2026021600036';
      document.getElementById('onlineModel').value = '';
      document.getElementById('onlineLimit').value = '20';
      loadOnlineData();
    });
    document.getElementById('createConfirmBtn').addEventListener('click', async () => {
      const data = await api('/api/cards/confirm', {
        method: 'POST',
        body: JSON.stringify({
          title: '是否进入模型晋升灰度评估？',
          question: '候选模型已完成日终分析，是否允许进入下一步灰度评估？'
        })
      });
      renderConfirmTask(data.task);
      const dryRun = data.send_result && data.send_result.dry_run;
      addMessage('agent', `已创建确认卡片任务：${data.task.task_id}\n${dryRun ? '当前为 dry-run，配置飞书凭证后会真实发送卡片。' : '已发送到飞书。'}`);
    });
    async function simulateChoice(choice) {
      if (!currentTask) {
        addMessage('agent', '请先点击“发送确认卡片”创建任务。');
        return;
      }
      lastChoice = choice;
      const data = await api('/api/cards/callback', {
        method: 'POST',
        body: JSON.stringify({ task_id: currentTask.task_id, action: choice, user_id: 'demo_user' })
      });
      if (data.task) renderConfirmTask(data.task);
      addMessage('agent', `Agent 收到交互信号：${data.signal}\n重复事件：${data.duplicate ? '是' : '否'}`);
    }
    document.getElementById('simulateYesBtn').addEventListener('click', () => simulateChoice('yes'));
    document.getElementById('simulateNoBtn').addEventListener('click', () => simulateChoice('no'));
    document.getElementById('duplicateClickBtn').addEventListener('click', () => simulateChoice(lastChoice));
    document.getElementById('sendFeishuBtn').addEventListener('click', async () => {
      const data = await api('/api/send-test-alert', { method: 'POST', body: JSON.stringify({}) });
      const mode = data.dry_run ? '未配置 webhook，已展示 dry-run payload' : '已请求飞书 webhook';
      addMessage('agent', `${mode}\n${JSON.stringify(data.body || data.payload, null, 2)}`);
    });
    document.getElementById('zoomRestorationBtn').addEventListener('click', () => openChartZoom('restorationCurveChart', '还原前后功率曲线'));
    document.getElementById('zoomCompareBtn').addEventListener('click', () => openChartZoom('modelEnergyCompareChart', '第一版 vs 迭代版多发电量对比'));
    document.getElementById('zoomRegionBtn').addEventListener('click', () => openChartZoom('regionChart', '区域处理表现'));
    document.getElementById('zoomIssueBtn').addEventListener('click', () => openChartZoom('issueChart', '问题归因 Pareto'));
    document.getElementById('restorationCurveChart').addEventListener('click', () => openChartZoom('restorationCurveChart', '还原前后功率曲线'));
    document.getElementById('modelEnergyCompareChart').addEventListener('click', () => openChartZoom('modelEnergyCompareChart', '第一版 vs 迭代版多发电量对比'));
    document.getElementById('regionChart').addEventListener('click', () => openChartZoom('regionChart', '区域处理表现'));
    document.getElementById('issueChart').addEventListener('click', () => openChartZoom('issueChart', '问题归因 Pareto'));
    document.getElementById('closeZoomBtn').addEventListener('click', closeChartZoom);
    document.getElementById('chartZoomOverlay').addEventListener('click', (event) => {
      if (event.target.id === 'chartZoomOverlay') closeChartZoom();
    });
    window.addEventListener('keydown', (event) => {
      if (event.key === 'Escape') closeChartZoom();
    });

    connectEvents();
    refresh().catch(err => {
      document.body.insertAdjacentHTML('afterbegin', `<pre style="color:#c93636;padding:16px">${err.message}</pre>`);
    });
  </script>
</body>
</html>
"""


def _json_bytes(payload: Dict[str, Any], status: int = 200) -> Tuple[int, bytes, str]:
    return (
        status,
        json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        "application/json; charset=utf-8",
    )


class DemoHandler(BaseHTTPRequestHandler):
    server_version = "PVClippingFeishuAgentDemo/1.0"

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid json: {exc}") from exc

    def _handle_websocket(self) -> None:
        key = self.headers.get("Sec-WebSocket-Key")
        if not key:
            self._send(*_json_bytes({"error": "missing Sec-WebSocket-Key"}, status=HTTPStatus.BAD_REQUEST))
            return

        accept = base64.b64encode(
            hashlib.sha1((key + WS_GUID).encode("ascii")).digest()
        ).decode("ascii")
        self.send_response(HTTPStatus.SWITCHING_PROTOCOLS)
        self.send_header("Upgrade", "websocket")
        self.send_header("Connection", "Upgrade")
        self.send_header("Sec-WebSocket-Accept", accept)
        self.end_headers()

        sock = self.request
        with WS_LOCK:
            WS_CLIENTS.add(sock)
        _ws_send(
            sock,
            {
                "kind": "ws.connected",
                "payload": {"recent_events": list_agent_events(limit=20)},
                "sent_at": time.time(),
            },
        )
        try:
            while True:
                time.sleep(25)
                if not _ws_send(
                    sock,
                    {"kind": "ws.heartbeat", "payload": {}, "sent_at": time.time()},
                ):
                    break
        finally:
            with WS_LOCK:
                WS_CLIENTS.discard(sock)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._send(HTTPStatus.NO_CONTENT, b"", "text/plain")

    def do_GET(self) -> None:  # noqa: N802
        parsed_url = urlparse(self.path)
        path = parsed_url.path
        if path == "/ws/events":
            self._handle_websocket()
            return
        if path in ("/", "/index.html"):
            self._send(HTTPStatus.OK, INDEX_HTML.encode("utf-8"), "text/html; charset=utf-8")
            return
        if path == "/api/status":
            self._send(*_json_bytes(build_monitor_snapshot()))
            return
        if path == "/api/report":
            self._send(*_json_bytes({"report": build_daily_report()}))
            return
        if path == "/api/events":
            self._send(*_json_bytes({"events": list_agent_events(limit=100)}))
            return
        if path == "/api/online/clipping-summary":
            from urllib.parse import parse_qs

            query = parse_qs(parsed_url.query)
            date_str = (query.get("date") or query.get("dt") or ["2026-05-22"])[0]
            station_id = (query.get("station_id") or [None])[0]
            model_name = (query.get("model_name") or [None])[0]
            limit = int((query.get("limit") or ["20"])[0])
            include_payload = (query.get("include_payload") or ["0"])[0] in ("1", "true", "True")
            try:
                summary = query_online_clipping_summary(
                    date_str=date_str,
                    model_name=model_name or None,
                )
                station_history = None
                if station_id:
                    station_history = query_online_station_history(
                        station_id=station_id,
                        date_str=date_str,
                        limit=limit,
                        include_payload=include_payload,
                    )
                self._send(
                    *_json_bytes(
                        {
                            "ok": True,
                            "summary": summary,
                            "station_history": station_history,
                        }
                    )
                )
            except Exception as exc:
                self._send(
                    *_json_bytes(
                        {
                            "ok": False,
                            "error": type(exc).__name__,
                            "detail": str(exc),
                            "hint": "检查 StarRocks 9030 网络、Nacos snapshot/env 配置和表字段。",
                        },
                        status=HTTPStatus.BAD_GATEWAY,
                    )
                )
            return
        if path == "/api/online/restoration-curve":
            from urllib.parse import parse_qs

            query = parse_qs(parsed_url.query)
            date_str = (query.get("date") or query.get("dt") or ["2026-05-22"])[0]
            station_id = (query.get("station_id") or [""])[0]
            model_name = (query.get("model_name") or [None])[0]
            if not station_id:
                self._send(
                    *_json_bytes(
                        {"ok": False, "error": "missing station_id"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                )
                return
            try:
                result = query_online_restoration_curve(
                    station_id=station_id,
                    date_str=date_str,
                    model_name=model_name or None,
                )
                self._send(*_json_bytes(result))
            except Exception as exc:
                self._send(
                    *_json_bytes(
                        {
                            "ok": False,
                            "error": type(exc).__name__,
                            "detail": str(exc),
                            "hint": "检查 station_statistics_min 原始功率、pv_prediction_history 还原结果和 station_id/date。",
                        },
                        status=HTTPStatus.BAD_GATEWAY,
                    )
                )
            return
        if path == "/api/k8s/log-summary":
            from urllib.parse import parse_qs

            query = parse_qs(parsed_url.query)
            namespace = (query.get("namespace") or ["data-platform"])[0]
            keywords = parse_keywords((query.get("keywords") or ["sigen-pv-clipping"])[0])
            limit_pods = int((query.get("limit_pods") or ["8"])[0])
            tail_lines = int((query.get("tail_lines") or ["500"])[0])
            container = (query.get("container") or [None])[0]
            previous = (query.get("previous") or ["0"])[0] in ("1", "true", "True")
            try:
                result = query_k8s_log_summary(
                    namespace=namespace,
                    keywords=keywords,
                    limit_pods=limit_pods,
                    tail_lines=tail_lines,
                    container=container or None,
                    previous=previous,
                )
                self._send(*_json_bytes(result))
            except Exception as exc:
                self._send(
                    *_json_bytes(
                        {
                            "ok": False,
                            "error": type(exc).__name__,
                            "detail": str(exc),
                            "hint": "检查 PV_CLIPPING_K8S_TOKEN、Dashboard URL、namespace 和 RBAC 日志权限。",
                        },
                        status=HTTPStatus.BAD_GATEWAY,
                    )
                )
            return
        if path == "/health":
            self._send(*_json_bytes({"ok": True, "service": self.server_version}))
            return
        self._send(*_json_bytes({"error": "not found"}, status=HTTPStatus.NOT_FOUND))

    def do_POST(self) -> None:  # noqa: N802
        try:
            path = urlparse(self.path).path
            payload = self._read_json()
            if path == "/api/ask":
                answer = answer_question(payload.get("question", ""))
                self._send(*_json_bytes({"answer": answer}))
                return
            if path == "/api/simulate-run":
                result = simulate_daily_run(payload.get("area", "Europe/*"))
                self._send(*_json_bytes(result))
                return
            if path == "/api/send-test-alert":
                notifier = FeishuNotifier()
                result = notifier.send_webhook_card(build_feishu_card())
                self._send(*_json_bytes(result))
                return
            if path == "/api/cards/confirm":
                result = send_confirmation_card(
                    chat_id=payload.get("chat_id"),
                    title=payload.get("title") or "是否进入模型晋升灰度评估？",
                    question=payload.get("question") or "候选模型已完成日终分析，是否进入下一步灰度评估？",
                    context=payload.get("context") or {},
                    notifier=FeishuNotifier(),
                )
                broadcast_ws("card.sent", result)
                self._send(*_json_bytes(result))
                return
            if path == "/api/cards/callback":
                result = simulate_card_callback(
                    task_id=payload.get("task_id", ""),
                    action=payload.get("action", "yes"),
                    user_id=payload.get("user_id", "demo_user"),
                )
                broadcast_ws("card.callback", result)
                self._send(*_json_bytes(result))
                return
            if path in ("/feishu/events", "/feishu/webhook"):
                notifier = FeishuNotifier()
                result = handle_feishu_event(payload, notifier)
                broadcast_ws("feishu.event", result)
                self._send(*_json_bytes(result))
                return
            self._send(*_json_bytes({"error": "not found"}, status=HTTPStatus.NOT_FOUND))
        except ValueError as exc:
            self._send(*_json_bytes({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST))
        except Exception as exc:  # Keep the demo service responsive during live demos.
            self._send(
                *_json_bytes({"error": type(exc).__name__, "detail": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            )

    def log_message(self, fmt: str, *args: Any) -> None:
        if os.getenv("PV_CLIPPING_AGENT_ACCESS_LOG", "0") == "1":
            super().log_message(fmt, *args)


def main() -> None:
    parser = argparse.ArgumentParser(description="PV clipping Feishu Agent demo server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8765, type=int)
    args = parser.parse_args()

    httpd = ThreadingHTTPServer((args.host, args.port), DemoHandler)
    print(f"PV clipping Feishu Agent demo: http://{args.host}:{args.port}", flush=True)
    print("Feishu event callback path: /feishu/events", flush=True)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
