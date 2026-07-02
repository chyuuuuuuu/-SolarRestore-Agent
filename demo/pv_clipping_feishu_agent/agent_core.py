"""Monitoring, Q&A, and Feishu integration core for the PV clipping demo."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .data import (
    AGENTS,
    EUROPE_SUMMARY,
    EVOLUTION_STEPS,
    ISSUES,
    MODELS,
    PROJECT,
    REGIONS,
    SUGGESTED_QUESTIONS,
)

_STATE_LOCK = threading.RLock()
_EVENT_LOG: List[Dict[str, Any]] = []
_CARD_TASKS: Dict[str, Dict[str, Any]] = {}
_PROCESSED_EVENTS = set()

SAMPLE_BUCKET_POLICIES: Dict[str, Dict[str, str]] = {
    "data_quality": {
        "sample_pool": "quality.power_interpolation",
        "scene_label": "power_interpolation_failed",
        "target_dataset": "data_quality_set",
        "train_policy": "quality_feature",
        "action": "进入功率插值质量集，并生成训练前插值稳定性规则",
    },
    "duplicate_index": {
        "sample_pool": "preprocess.duplicate_timestamp",
        "scene_label": "duplicate_timestamp_index",
        "target_dataset": "preprocess_rule_set",
        "train_policy": "train_after_dedup",
        "action": "进入重复 timestamp 场景，训练前按时间戳去重或聚合",
    },
    "code_contract": {
        "sample_pool": "contract.code_bug",
        "scene_label": "engineering_contract_violation",
        "target_dataset": "contract_blocklist",
        "train_policy": "blocked_from_training",
        "action": "进入工程契约修复队列，不进入模型训练集",
    },
    "missing_power": {
        "sample_pool": "availability.missing_power",
        "scene_label": "missing_power_availability",
        "target_dataset": "availability_report",
        "train_policy": "not_negative_sample",
        "action": "标记数据不可用，不作为负样本训练",
    },
    "missing_weather": {
        "sample_pool": "feature.missing_weather",
        "scene_label": "missing_weather_feature",
        "target_dataset": "feature_missing_set",
        "train_policy": "train_with_missing_flag",
        "action": "记录 weather_source 缺失，增加天气缺失兜底与特征缺失标志位",
    },
    "mask_boundary": {
        "sample_pool": "scenario.mask_boundary",
        "scene_label": "short_day_or_boundary_clipping",
        "target_dataset": "scenario_extension_set",
        "train_policy": "train_new_scene",
        "action": "构建非 288 点、短日曲线和边界点削峰新场景样本",
    },
    "interface_contract": {
        "sample_pool": "contract.interface",
        "scene_label": "mask_scene_vector_contract",
        "target_dataset": "contract_blocklist",
        "train_policy": "blocked_from_training",
        "action": "固定 mask 与 scene_vector 返回结构，契约测试通过后再释放样本",
    },
}

TRAINABLE_POLICIES = {"train_after_dedup", "train_with_missing_flag", "train_new_scene", "quality_feature"}
BLOCKED_POLICIES = {"blocked_from_training"}
HOLDOUT_POLICIES = {"not_negative_sample"}


def _pct(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return round(numerator * 100.0 / denominator, 2)


def _fmt_pct(value: float) -> str:
    return f"{value:.2f}%"


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def record_agent_event(kind: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    event = {
        "id": _new_id("evt"),
        "kind": kind,
        "created_at": _now_iso(),
        "payload": payload,
    }
    with _STATE_LOCK:
        _EVENT_LOG.append(event)
        del _EVENT_LOG[:-200]
    return event


def list_agent_events(limit: int = 50) -> List[Dict[str, Any]]:
    with _STATE_LOCK:
        return list(_EVENT_LOG[-limit:])


def list_card_tasks() -> List[Dict[str, Any]]:
    with _STATE_LOCK:
        return [dict(task) for task in _CARD_TASKS.values()]


def _with_region_rates(region: Dict[str, Any]) -> Dict[str, Any]:
    item = dict(region)
    item["potential_ratio"] = _pct(item["potential_clipping"], item["total_stations"])
    item["success_rate"] = _pct(item["success"], item["potential_clipping"])
    item["failure_rate"] = _pct(item["failed_or_skipped"], item["potential_clipping"])
    return item


def _build_alerts(summary: Dict[str, Any], issues: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    success_rate = _pct(summary["success_stations"], summary["potential_clipping_stations"])
    potential_ratio = _pct(summary["potential_clipping_stations"], summary["total_stations"])
    top_issue = max(issues, key=lambda item: item["count"])
    alerts = []

    if success_rate < 40:
        alerts.append(
            {
                "level": "critical",
                "title": "潜在削峰站点内成功率偏低",
                "detail": f"成功率 {_fmt_pct(success_rate)}，需要优先排查失败/跳过链路。",
            }
        )
    if potential_ratio > 50:
        alerts.append(
            {
                "level": "warning",
                "title": "潜在削峰占比较高",
                "detail": f"潜在削峰占比 {_fmt_pct(potential_ratio)}，建议关注区域策略变化。",
            }
        )
    if top_issue["count"] >= 1000:
        alerts.append(
            {
                "level": "warning",
                "title": "数据质量问题集中",
                "detail": f"{top_issue['type']} 出现 {top_issue['count']} 次，应进入样本回流规则。",
            }
        )
    alerts.append(
        {
            "level": "info",
            "title": "新旧模型可复用同一份输入灰度",
            "detail": "候选模型与基线模型共享功率、气象、配置、mask 和 scene_vector。",
        }
    )
    return alerts


def build_sample_feedback(issues: List[Dict[str, Any]], regions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build the implemented high-value sample feedback result.

    The demo uses daily issue buckets as the source of truth. Each bucket is
    routed into a stable sample pool with an explicit training policy so bad
    engineering-contract records do not pollute model training.
    """
    rows: List[Dict[str, Any]] = []
    for issue in sorted(issues, key=lambda item: item["count"], reverse=True):
        bucket = issue["bucket"]
        policy = SAMPLE_BUCKET_POLICIES.get(bucket, {})
        rows.append(
            {
                "sample_pool_id": f"hv-{PROJECT['sample_date'].replace('-', '')}-{bucket}",
                "issue_type": issue["type"],
                "bucket": bucket,
                "source_count": issue["count"],
                "selected_count": issue["count"],
                "priority": "P0" if issue["count"] >= 700 else "P1" if issue["count"] >= 100 else "P2",
                "scene_label": policy.get("scene_label", bucket),
                "target_dataset": policy.get("target_dataset", "feedback_set"),
                "train_policy": policy.get("train_policy", "review_required"),
                "action": policy.get("action", issue["action"]),
            }
        )

    total = sum(row["selected_count"] for row in rows)
    trainable = sum(row["selected_count"] for row in rows if row["train_policy"] in TRAINABLE_POLICIES)
    blocked = sum(row["selected_count"] for row in rows if row["train_policy"] in BLOCKED_POLICIES)
    holdout = sum(row["selected_count"] for row in rows if row["train_policy"] in HOLDOUT_POLICIES)
    top_regions = sorted(regions, key=lambda item: item["failed_or_skipped"], reverse=True)[:3]
    return {
        "status": "implemented",
        "generated_at": _now_iso(),
        "source": "daily_analysis_agent",
        "date": PROJECT["sample_date"],
        "sample_pool_version": f"hv_feedback_{PROJECT['sample_date'].replace('-', '')}_v1",
        "total_feedback_records": total,
        "trainable_records": trainable,
        "quality_or_holdout_records": holdout,
        "contract_blocked_records": blocked,
        "top_regions": [
            {
                "area": region["area"],
                "failed_or_skipped": region["failed_or_skipped"],
                "success_rate": region.get("success_rate", _pct(region["success"], region["potential_clipping"])),
            }
            for region in top_regions
        ],
        "buckets": rows,
    }


def build_dataset_reconstruction(sample_feedback: Dict[str, Any]) -> Dict[str, Any]:
    """Build the implemented dataset and scene-label reconstruction result."""
    buckets = sample_feedback["buckets"]
    label_rows = [
        {
            "label_id": idx + 1,
            "scene_label": row["scene_label"],
            "source_bucket": row["bucket"],
            "sample_count": row["selected_count"],
            "train_policy": row["train_policy"],
            "target_dataset": row["target_dataset"],
        }
        for idx, row in enumerate(buckets)
    ]
    trainable = sample_feedback["trainable_records"]
    train_count = int(trainable * 0.7)
    validation_count = int(trainable * 0.15)
    gray_eval_count = trainable - train_count - validation_count
    return {
        "status": "implemented",
        "generated_at": _now_iso(),
        "dataset_version": f"pv_clipping_feedback_{PROJECT['sample_date'].replace('-', '')}_v1",
        "source_sample_pool_version": sample_feedback["sample_pool_version"],
        "trainable_records": trainable,
        "splits": {
            "train": train_count,
            "validation": validation_count,
            "gray_eval": gray_eval_count,
            "quality_holdout": sample_feedback["quality_or_holdout_records"],
            "contract_blocklist": sample_feedback["contract_blocked_records"],
        },
        "scene_labels": label_rows,
        "preprocess_rules": [
            "timestamp/index 唯一化：重复点按 station_id + statistics_time 聚合",
            "非 288 点曲线强校验：短日曲线进入 short_day_or_boundary_clipping 标签",
            "无功率数据只进入可用性报表，不作为负样本训练",
            "气象空表增加 missing_weather_feature 标志位和跳过策略",
            "mask 与 scene_vector 返回结构固定为统一契约，契约异常进入 blocklist",
        ],
        "feature_contract": {
            "power": "288 点 5 分钟功率序列",
            "weather": "NWP 气象特征，支持 missing flag",
            "config": "站点配置与容量约束",
            "mask": "clipping_mask 长度必须与功率序列一致",
            "scene_vector": "PCS / 电网卖电限制 / 负载侧约束 / 数据质量标签",
        },
        "next_model_target": MODELS["candidate"]["name"],
    }


def build_monitor_snapshot() -> Dict[str, Any]:
    """Return a complete monitoring snapshot for the demo UI and Feishu cards."""
    summary = dict(EUROPE_SUMMARY)
    summary["potential_ratio"] = _pct(
        summary["potential_clipping_stations"], summary["total_stations"]
    )
    summary["success_rate"] = _pct(
        summary["success_stations"], summary["potential_clipping_stations"]
    )
    summary["failed_rate"] = _pct(
        summary["failed_or_skipped_stations"], summary["potential_clipping_stations"]
    )

    regions = [_with_region_rates(region) for region in REGIONS]
    issues = sorted((dict(issue) for issue in ISSUES), key=lambda item: item["count"], reverse=True)
    sample_feedback = build_sample_feedback(issues, regions)
    dataset_reconstruction = build_dataset_reconstruction(sample_feedback)

    return {
        "project": PROJECT,
        "generated_at": _now_iso(),
        "summary": summary,
        "regions": regions,
        "issues": issues,
        "alerts": _build_alerts(summary, issues),
        "agents": AGENTS,
        "models": MODELS,
        "evolution_steps": EVOLUTION_STEPS,
        "sample_feedback": sample_feedback,
        "dataset_reconstruction": dataset_reconstruction,
        "suggested_questions": SUGGESTED_QUESTIONS,
        "card_tasks": list_card_tasks(),
        "recent_events": list_agent_events(limit=20),
    }


def _find_region(question: str, regions: Iterable[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    normalized = question.lower()
    aliases = {
        "berlin": ["berlin", "柏林"],
        "london": ["london", "伦敦"],
        "madrid": ["madrid", "马德里"],
        "sarajevo": ["sarajevo", "萨拉热窝"],
        "helsinki": ["helsinki", "赫尔辛基"],
    }
    for region in regions:
        tail = region["area"].split("/")[-1].lower()
        terms = aliases.get(tail, [tail])
        if any(term in normalized for term in terms) or region["area"].lower() in normalized:
            return region
    return None


def _issue_lines(issues: List[Dict[str, Any]], limit: int = 5) -> List[str]:
    return [
        f"{idx}. {issue['type']}：{issue['count']} 次，处理动作：{issue['action']}"
        for idx, issue in enumerate(issues[:limit], start=1)
    ]


def answer_question(question: str, snapshot: Optional[Dict[str, Any]] = None) -> str:
    """Rule-based Q&A for competition demos and Feishu chat callbacks."""
    snapshot = snapshot or build_monitor_snapshot()
    question = (question or "").strip()
    normalized = question.lower()
    summary = snapshot["summary"]
    issues = snapshot["issues"]
    region = _find_region(question, snapshot["regions"])

    if not question:
        return "请直接问我削峰还原状态、失败原因、区域风险、新旧模型灰度或样本回流。"

    if re.search(r"线上|9030|实际|入库|多少.*站|predict_type|pv_prediction_history", question):
        date_match = re.search(r"\d{4}-\d{2}-\d{2}", question)
        station_match = re.search(r"station_id\s*[=：:]?\s*['\"]?(\d{8,})", question)
        if not station_match:
            station_match = re.search(r"\b(\d{10,})\b", question)
        date_str = date_match.group(0) if date_match else datetime.now().strftime("%Y-%m-%d")
        station_id = station_match.group(1) if station_match else None
        try:
            from .online_data import query_online_clipping_summary, query_online_station_history

            summary = query_online_clipping_summary(date_str=date_str)
            answer = (
                f"线上 StarRocks 9030 查询结果：{date_str}、predict_type=4 下，"
                f"`sigen_device.pv_prediction_history` 有 "
                f"{summary['restored_station_count']:,} 个去重站点产生削峰还原结果，"
                f"记录行数 {summary['row_count']:,}，模型数 {summary['model_count']:,}。"
            )
            if station_id:
                history = query_online_station_history(station_id=station_id, date_str=date_str, limit=5)
                answer += (
                    f"\n示例站点 {station_id} 在该日期命中 {history['row_count']} 条 predict_type=4 记录。"
                )
            return answer
        except Exception as exc:
            return (
                f"线上 9030 查询失败：{type(exc).__name__}: {exc}。"
                "请检查 StarRocks 网络、Nacos snapshot/env 配置和表字段。"
            )

    if re.search(r"k8s|kubernetes|容器|日志|log|data-platform|sigen-pv-clipping|clipping.*eff|eff.*clipping", normalized):
        namespace_match = re.search(r"namespace\s*[=：:]?\s*([a-zA-Z0-9_-]+)", question)
        namespace = namespace_match.group(1) if namespace_match else "data-platform"
        try:
            from .k8s_logs import parse_keywords, query_k8s_log_summary

            keywords = parse_keywords("sigen-pv-clipping")
            result = query_k8s_log_summary(namespace=namespace, keywords=keywords, limit_pods=6, tail_lines=500)
            analysis = result["analysis"]
            severity = analysis["severity_counts"]
            issue_counts = analysis["issue_counts"]
            top_issues = "\n".join(
                f"{idx}. {name}：{count} 次"
                for idx, (name, count) in enumerate(issue_counts.items(), start=1)
            ) or "未识别到已知问题桶。"
            return (
                f"K8s Dashboard 实时日志分析完成：namespace={namespace}，"
                f"匹配 Pod {result['matched_pods']} 个，本次读取 {len(result['log_sources'])} 个日志源，"
                f"共 {analysis['line_count']} 行。\n"
                f"严重级别：ERROR {severity.get('ERROR', 0)}，WARNING {severity.get('WARNING', 0)}，"
                f"INFO {severity.get('INFO', 0)}。\n"
                f"问题归因：\n{top_issues}"
            )
        except Exception as exc:
            return (
                f"K8s 日志分析失败：{type(exc).__name__}: {exc}。"
                "请检查 PV_CLIPPING_K8S_TOKEN、Dashboard URL、namespace 和 RBAC 日志权限。"
            )

    if region:
        return (
            f"{region['area']}：总站点 {region['total_stations']:,}，潜在削峰 "
            f"{region['potential_clipping']:,}（{_fmt_pct(region['potential_ratio'])}），"
            f"成功 {region['success']:,}，失败/跳过 {region['failed_or_skipped']:,}，"
            f"潜在站点内成功率 {_fmt_pct(region['success_rate'])}。"
            "建议优先看功率插值、重复 timestamp 和气象空表三类数据质量问题。"
        )

    if re.search(r"状态|概览|汇总|今天|今日|欧洲|运行|监控", question):
        return (
            f"{summary['date']} 欧洲 {summary['covered_regions']} 个区域共覆盖 "
            f"{summary['total_stations']:,} 个站点，识别潜在削峰 "
            f"{summary['potential_clipping_stations']:,} 个，占比 {_fmt_pct(summary['potential_ratio'])}。"
            f"成功还原 {summary['success_stations']:,} 个，失败/跳过 "
            f"{summary['failed_or_skipped_stations']:,} 个，潜在站点内成功率 "
            f"{_fmt_pct(summary['success_rate'])}。当前最需要关注的是数据质量和接口契约问题。"
        )

    if re.search(r"失败|错误|归因|问题|异常|原因", question):
        lines = _issue_lines(issues)
        return "当前自动归因 Top 问题：\n" + "\n".join(lines)

    if re.search(r"模型|灰度|10000|晋升|候选|基线|新旧", question):
        baseline = snapshot["models"]["baseline"]
        candidate = snapshot["models"]["candidate"]
        return (
            f"基线模型是 {baseline['name']}（{baseline['s3_key']}），候选模型是 "
            f"{candidate['name']}（{candidate['s3_key']}）。灰度机制是同站点、同日期、同一份功率/"
            "气象/配置/clipping_mask/scene_vector 输入并行推理，日终比较成功率、削峰点还原差异、"
            "异常率和区域稳定性；达标后候选模型晋升主模型，未达标样本继续回流。"
        )

    if re.search(r"数据集|样本|回流|训练|进化|闭环|迭代", question):
        feedback = snapshot["sample_feedback"]
        dataset = snapshot["dataset_reconstruction"]
        top_buckets = "\n".join(
            f"{idx}. {row['issue_type']} -> {row['scene_label']}：{row['selected_count']} 条，策略 {row['train_policy']}"
            for idx, row in enumerate(feedback["buckets"][:5], start=1)
        )
        return (
            f"高价值样本回流已实现：样本池版本 {feedback['sample_pool_version']}，"
            f"当日回流 {feedback['total_feedback_records']:,} 条问题记录，其中 "
            f"{feedback['trainable_records']:,} 条可进入训练/特征缺失训练，"
            f"{feedback['quality_or_holdout_records']:,} 条进入质量或可用性留存，"
            f"{feedback['contract_blocked_records']:,} 条工程契约问题被阻断训练。\n"
            f"数据集与场景标签重构已实现：数据集版本 {dataset['dataset_version']}，"
            f"train/validation/gray_eval = {dataset['splits']['train']:,}/"
            f"{dataset['splits']['validation']:,}/{dataset['splits']['gray_eval']:,}。\n"
            f"Top 回流桶：\n{top_buckets}"
        )

    if re.search(r"agent|架构|流程|链路|编排", normalized):
        online = [agent for agent in snapshot["agents"] if "已上线" in agent["status"]]
        gray = [agent for agent in snapshot["agents"] if "灰度" in agent["status"]]
        partial = [agent for agent in snapshot["agents"] if "规划" in agent["status"]]
        return (
            f"当前 Agent 架构覆盖编排、数据接入、检测/场景、还原、下发、分析、进化 7 类职责。"
            f"已上线 {len(online)} 类，灰度 {len(gray)} 类，规划/部分实现 {len(partial)} 类。"
            "生产链路是数据接入 -> 潜在削峰识别 -> 场景判定 -> 双模型还原 -> Kafka 下发 -> 日终分析 -> 进化回流。"
        )

    if re.search(r"kafka|下发|topic|pv_result", normalized):
        return (
            f"削峰还原结果通过 Kafka {snapshot['project']['kafka_topic']} 下发。"
            "消息包含 date、station_id、predict_type、model_name、confidence、record_time、时间戳数组和还原功率数组。"
        )

    if re.search(r"s3|发布|checkpoint|pth", normalized):
        return (
            "当前基线和候选模型都按 S3 key 管理。候选模型发布后先作为灰度模型并行推理，"
            "只有在日终指标达标后才晋升为主模型，避免一次性替换线上模型。"
        )

    if re.search(r"亮点|比赛|价值|答辩|总结", question):
        return (
            "答辩重点可以概括为：这不是单次训练模型，而是把削峰还原升级成生产级 AI 自动闭环。"
            "AI 贯穿检测、场景判断、还原、分析和进化；欧洲单日覆盖 29,456 个站点；"
            "线上问题能自动归因并回流为数据集和模型版本迭代。当前高价值样本回流、数据集与场景标签重构已实现；"
            "自动训练、S3 发布和晋升策略仍在规划/部分实现。"
        )

    if re.search(r"风险|边界|todo|下一步|缺口", normalized):
        return (
            "下一步优先级：1. 修复 calculate_clipping_mask 返回契约并加测试；"
            "2. 数据接入层去重并标记非 288 点曲线；3. 气象空表提前拦截；"
            "4. 把日终归因结果稳定写入样本池；5. 完善候选模型晋升阈值和回滚策略。"
        )

    return (
        "我可以回答削峰还原运行状态、区域风险、失败归因、新旧模型灰度、样本回流和比赛答辩亮点。"
        "你可以问：" + "；".join(snapshot["suggested_questions"][:4])
    )


def build_daily_report(snapshot: Optional[Dict[str, Any]] = None) -> str:
    snapshot = snapshot or build_monitor_snapshot()
    summary = snapshot["summary"]
    top_regions = sorted(
        snapshot["regions"], key=lambda item: item["failed_or_skipped"], reverse=True
    )[:3]
    lines = [
        f"PV 削峰还原日终报告（{summary['date']}）",
        "",
        f"- 覆盖区域：{summary['covered_regions']} 个",
        f"- 覆盖站点：{summary['total_stations']:,} 个",
        f"- 潜在削峰：{summary['potential_clipping_stations']:,} 个（{_fmt_pct(summary['potential_ratio'])}）",
        f"- 成功还原：{summary['success_stations']:,} 个",
        f"- 失败/跳过：{summary['failed_or_skipped_stations']:,} 个",
        f"- 潜在站点内成功率：{_fmt_pct(summary['success_rate'])}",
        "",
        "重点区域：",
    ]
    lines.extend(
        [
            f"- {region['area']}：失败/跳过 {region['failed_or_skipped']:,}，成功率 {_fmt_pct(region['success_rate'])}"
            for region in top_regions
        ]
    )
    lines.extend(["", "自动归因 Top 问题："])
    lines.extend([f"- {issue['type']}：{issue['count']} 次" for issue in snapshot["issues"][:5]])
    lines.extend(
        [
            "",
            "闭环动作：",
            "- 数据质量问题进入样本池和前处理规则",
            "- 工程契约问题先进入修复队列，不污染训练集",
            "- 候选模型继续与基线模型共享输入并行灰度评估",
        ]
    )
    return "\n".join(lines)


def build_feishu_card(snapshot: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    snapshot = snapshot or build_monitor_snapshot()
    summary = snapshot["summary"]
    top_issue = snapshot["issues"][0]
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "red" if summary["success_rate"] < 40 else "orange",
            "title": {"tag": "plain_text", "content": "PV 削峰还原日终监控"},
        },
        "elements": [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        f"**日期**：{summary['date']}\n"
                        f"**覆盖站点**：{summary['total_stations']:,}\n"
                        f"**潜在削峰**：{summary['potential_clipping_stations']:,} "
                        f"({_fmt_pct(summary['potential_ratio'])})\n"
                        f"**成功还原**：{summary['success_stations']:,}\n"
                        f"**潜在站点内成功率**：{_fmt_pct(summary['success_rate'])}"
                    ),
                },
            },
            {"tag": "hr"},
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**Top 问题**：{top_issue['type']}，{top_issue['count']} 次\n**建议动作**：{top_issue['action']}",
                },
            },
            {
                "tag": "note",
                "elements": [
                    {
                        "tag": "plain_text",
                        "content": "候选模型与基线模型复用同一份输入，继续灰度并行评估。",
                    }
                ],
            },
        ],
    }


def build_confirmation_card(task: Dict[str, Any]) -> Dict[str, Any]:
    """Build an interactive Feishu card for yes/no confirmation.

    Pending cards contain two buttons. Answered cards remove the buttons so the
    same message can be patched in place after the callback is received.
    """
    status = task.get("status", "pending")
    choice = task.get("choice")
    pending = status == "pending"
    template = "blue" if pending else ("green" if choice == "yes" else "red")
    status_line = (
        "等待用户选择"
        if pending
        else f"已选择：{'Yes / 确认' if choice == 'yes' else 'No / 拒绝'}"
    )
    elements: List[Dict[str, Any]] = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"**任务**：{task['title']}\n"
                    f"**问题**：{task['question']}\n"
                    f"**状态**：{status_line}"
                ),
            },
        }
    ]
    if pending:
        elements.append(
            {
                "tag": "action",
                "actions": [
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "Yes"},
                        "type": "primary",
                        "value": {
                            "task_id": task["task_id"],
                            "card_id": task["card_id"],
                            "action": "yes",
                        },
                    },
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "No"},
                        "type": "danger",
                        "value": {
                            "task_id": task["task_id"],
                            "card_id": task["card_id"],
                            "action": "no",
                        },
                    },
                ],
            }
        )
    else:
        elements.append(
            {
                "tag": "note",
                "elements": [
                    {
                        "tag": "plain_text",
                        "content": "原卡片已更新，按钮已移除，重复点击不会再次触发业务动作。",
                    }
                ],
            }
        )

    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": template,
            "title": {"tag": "plain_text", "content": "PV 削峰还原 Agent 确认"},
        },
        "elements": elements,
    }


def create_confirmation_task(
    title: str = "是否进入模型晋升灰度评估？",
    question: str = "候选模型已完成日终分析，是否进入下一步灰度评估？",
    chat_id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    task_id = _new_id("task")
    task = {
        "task_id": task_id,
        "card_id": _new_id("card"),
        "title": title,
        "question": question,
        "chat_id": chat_id,
        "context": context or {},
        "status": "pending",
        "choice": None,
        "message_id": None,
        "update_token": None,
        "created_at": _now_iso(),
        "answered_at": None,
        "answered_by": None,
        "event_key": None,
    }
    with _STATE_LOCK:
        _CARD_TASKS[task_id] = task
    event = record_agent_event(
        "card.created",
        {
            "task_id": task_id,
            "card_id": task["card_id"],
            "title": title,
            "question": question,
        },
    )
    return {"task": dict(task), "card": build_confirmation_card(task), "event": event}


def _extract_nested(payload: Dict[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _first_value(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def parse_card_action(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    event = payload.get("event", payload)
    action_obj = _first_value(event.get("action"), payload.get("action"), {})
    action_value = action_obj.get("value") if isinstance(action_obj, dict) else {}
    if isinstance(action_value, str):
        try:
            action_value = json.loads(action_value)
        except json.JSONDecodeError:
            action_value = {"action": action_value}
    if not isinstance(action_value, dict):
        action_value = {}

    action = _first_value(
        action_value.get("action"),
        action_value.get("choice"),
        action_obj.get("option") if isinstance(action_obj, dict) else None,
        payload.get("action"),
    )
    if isinstance(action, str):
        action = action.lower().strip()
    if action not in ("yes", "no"):
        return None

    user_id = _first_value(
        _extract_nested(event, "operator", "operator_id", "open_id"),
        _extract_nested(event, "operator", "operator_id", "user_id"),
        _extract_nested(event, "operator", "open_id"),
        _extract_nested(payload, "operator", "open_id"),
        "demo_user",
    )
    message_id = _first_value(
        _extract_nested(event, "context", "open_message_id"),
        event.get("open_message_id"),
        payload.get("open_message_id"),
        payload.get("message_id"),
    )
    update_token = _first_value(event.get("token"), payload.get("token"))
    task_id = _first_value(action_value.get("task_id"), payload.get("task_id"))
    card_id = _first_value(action_value.get("card_id"), payload.get("card_id"))
    event_id = _first_value(
        _extract_nested(payload, "header", "event_id"),
        payload.get("event_id"),
        f"{task_id or 'unknown'}:{user_id}:{action}:{message_id or card_id or 'local'}",
    )
    return {
        "action": action,
        "task_id": task_id,
        "card_id": card_id,
        "user_id": user_id,
        "message_id": message_id,
        "update_token": update_token,
        "event_id": event_id,
        "raw_action_value": action_value,
    }


def apply_card_action(parsed: Dict[str, Any]) -> Dict[str, Any]:
    action = parsed["action"]
    task_id = parsed.get("task_id")
    event_key = parsed.get("event_id")
    with _STATE_LOCK:
        if event_key in _PROCESSED_EVENTS:
            task = dict(_CARD_TASKS.get(task_id, {})) if task_id else {}
            event = record_agent_event(
                "card.duplicate_ignored",
                {"event_id": event_key, "task_id": task_id, "action": action},
            )
            return {
                "ok": True,
                "duplicate": True,
                "signal": task.get("choice") or action,
                "task": task,
                "event": event,
            }

        task = _CARD_TASKS.get(task_id) if task_id else None
        if not task:
            event = record_agent_event(
                "card.unknown_task",
                {"event_id": event_key, "task_id": task_id, "action": action},
            )
            return {
                "ok": False,
                "duplicate": False,
                "signal": action,
                "error": "unknown task_id",
                "event": event,
            }

        _PROCESSED_EVENTS.add(event_key)
        if task.get("status") == "answered":
            event = record_agent_event(
                "card.already_answered",
                {
                    "event_id": event_key,
                    "task_id": task_id,
                    "old_choice": task.get("choice"),
                    "new_action": action,
                },
            )
            return {
                "ok": True,
                "duplicate": True,
                "signal": task.get("choice"),
                "task": dict(task),
                "event": event,
            }

        task["status"] = "answered"
        task["choice"] = action
        task["answered_at"] = _now_iso()
        task["answered_by"] = parsed.get("user_id")
        task["event_key"] = event_key
        task["message_id"] = parsed.get("message_id") or task.get("message_id")
        task["update_token"] = parsed.get("update_token") or task.get("update_token")
        event = record_agent_event(
            "card.choice_received",
            {
                "event_id": event_key,
                "task_id": task_id,
                "card_id": task.get("card_id"),
                "choice": action,
                "user_id": parsed.get("user_id"),
                "agent_signal": action,
            },
        )
        return {
            "ok": True,
            "duplicate": False,
            "signal": action,
            "task": dict(task),
            "card": build_confirmation_card(task),
            "event": event,
        }


def handle_card_action(
    payload: Dict[str, Any], notifier: Optional["FeishuNotifier"] = None
) -> Dict[str, Any]:
    parsed = parse_card_action(payload)
    if not parsed:
        return {"ok": False, "error": "not a yes/no card action"}

    result = apply_card_action(parsed)
    task = result.get("task") or {}
    card = build_confirmation_card(task) if task else result.get("card")
    notifier = notifier or FeishuNotifier()
    update_result = None
    if task and card and not result.get("duplicate"):
        update_result = notifier.update_card(
            card=card,
            message_id=task.get("message_id"),
            update_token=task.get("update_token"),
            card_id=task.get("card_id"),
        )
    result["parsed"] = parsed
    result["updated_card"] = card
    result["update_result"] = update_result
    return result


def simulate_card_callback(task_id: str, action: str, user_id: str = "demo_user") -> Dict[str, Any]:
    task = _CARD_TASKS.get(task_id)
    payload = {
        "header": {"event_id": _new_id("feishu_evt")},
        "event": {
            "operator": {"operator_id": {"open_id": user_id}},
            "context": {"open_message_id": task.get("message_id") if task else None},
            "action": {
                "value": {
                    "task_id": task_id,
                    "card_id": task.get("card_id") if task else None,
                    "action": action,
                }
            },
        },
    }
    return handle_card_action(payload, FeishuNotifier(dry_run=True))


def send_confirmation_card(
    chat_id: Optional[str] = None,
    title: str = "是否进入模型晋升灰度评估？",
    question: str = "候选模型已完成日终分析，是否进入下一步灰度评估？",
    context: Optional[Dict[str, Any]] = None,
    notifier: Optional["FeishuNotifier"] = None,
) -> Dict[str, Any]:
    created = create_confirmation_task(
        title=title, question=question, chat_id=chat_id, context=context
    )
    notifier = notifier or FeishuNotifier()
    if chat_id:
        send_result = notifier.send_chat_card(chat_id, created["card"])
    else:
        send_result = notifier.send_webhook_card(created["card"])

    message_id = _extract_nested(send_result, "body", "data", "message_id")
    if message_id:
        with _STATE_LOCK:
            task = _CARD_TASKS.get(created["task"]["task_id"])
            if task:
                task["message_id"] = message_id
                created["task"] = dict(task)
    event = record_agent_event(
        "card.sent",
        {
            "task_id": created["task"]["task_id"],
            "chat_id": chat_id,
            "dry_run": send_result.get("dry_run", False),
            "message_id": message_id,
        },
    )
    created["send_result"] = send_result
    created["send_event"] = event
    return created


def build_agent_context_state(task: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "task": task.get("title"),
        "task_id": task.get("task_id"),
        "card_status": task.get("status"),
        "user_choice": task.get("choice"),
        "next_step": "continue_execution" if task.get("choice") == "yes" else "stop_or_escalate",
    }


class FeishuNotifier:
    """Small Feishu client for demo webhooks and bot replies.

    Environment variables:
    FEISHU_WEBHOOK_URL / FEISHU_WEBHOOK_SECRET for custom bot push.
    FEISHU_APP_ID / FEISHU_APP_SECRET for replying to event callbacks.
    """

    def __init__(
        self,
        webhook_url: Optional[str] = None,
        webhook_secret: Optional[str] = None,
        app_id: Optional[str] = None,
        app_secret: Optional[str] = None,
        dry_run: Optional[bool] = None,
    ) -> None:
        self.webhook_url = webhook_url or os.getenv("FEISHU_WEBHOOK_URL")
        self.webhook_secret = webhook_secret or os.getenv("FEISHU_WEBHOOK_SECRET")
        self.app_id = app_id or os.getenv("FEISHU_APP_ID")
        self.app_secret = app_secret or os.getenv("FEISHU_APP_SECRET")
        if dry_run is None:
            dry_run = not bool(self.webhook_url or (self.app_id and self.app_secret))
        self.dry_run = dry_run
        self._tenant_token: Optional[Tuple[str, float]] = None

    @staticmethod
    def _sign_custom_bot(timestamp: str, secret: str) -> str:
        string_to_sign = f"{timestamp}\n{secret}"
        digest = hmac.new(
            string_to_sign.encode("utf-8"), digestmod=hashlib.sha256
        ).digest()
        return base64.b64encode(digest).decode("utf-8")

    def _post_json(
        self,
        url: str,
        payload: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 10,
        method: str = "POST",
    ) -> Dict[str, Any]:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req_headers = {"Content-Type": "application/json; charset=utf-8"}
        if headers:
            req_headers.update(headers)
        request = urllib.request.Request(url, data=data, headers=req_headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read().decode("utf-8")
                try:
                    parsed = json.loads(body) if body else {}
                except json.JSONDecodeError:
                    parsed = {"raw": body}
                return {"ok": True, "status": response.status, "body": parsed}
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            return {"ok": False, "status": exc.code, "body": body}
        except urllib.error.URLError as exc:
            return {"ok": False, "status": None, "body": str(exc)}

    def send_webhook_card(self, card: Dict[str, Any]) -> Dict[str, Any]:
        timestamp = str(int(time.time()))
        payload: Dict[str, Any] = {"msg_type": "interactive", "card": card}
        if self.webhook_secret:
            payload["timestamp"] = timestamp
            payload["sign"] = self._sign_custom_bot(timestamp, self.webhook_secret)
        if self.dry_run or not self.webhook_url:
            return {"ok": True, "dry_run": True, "payload": payload}
        result = self._post_json(self.webhook_url, payload)
        result["dry_run"] = False
        return result

    def send_chat_card(self, chat_id: str, card: Dict[str, Any]) -> Dict[str, Any]:
        if self.dry_run or not (self.app_id and self.app_secret):
            return {
                "ok": True,
                "dry_run": True,
                "payload": {"chat_id": chat_id, "msg_type": "interactive", "card": card},
            }
        token = self._get_tenant_access_token()
        if not token:
            return {"ok": False, "dry_run": False, "body": "tenant_access_token unavailable"}
        url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
        payload = {
            "receive_id": chat_id,
            "msg_type": "interactive",
            "content": json.dumps(card, ensure_ascii=False),
        }
        result = self._post_json(
            url, payload, headers={"Authorization": f"Bearer {token}"}
        )
        result["dry_run"] = False
        return result

    def update_card(
        self,
        card: Dict[str, Any],
        message_id: Optional[str] = None,
        update_token: Optional[str] = None,
        card_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Patch the original card if Feishu credentials are configured.

        Feishu deployments differ in whether callbacks provide a card update
        token or only an open_message_id. The demo supports both shapes and
        stays in dry-run mode when credentials or IDs are missing.
        """
        dry_payload = {
            "message_id": message_id,
            "update_token": update_token,
            "card_id": card_id,
            "card": card,
            "note": "dry-run: configure FEISHU_APP_ID/FEISHU_APP_SECRET and pass callback token or message_id to patch the original card",
        }
        if self.dry_run or not (self.app_id and self.app_secret):
            return {"ok": True, "dry_run": True, "payload": dry_payload}

        token = self._get_tenant_access_token()
        if not token:
            return {"ok": False, "dry_run": False, "body": "tenant_access_token unavailable"}

        override_url = os.getenv("FEISHU_CARD_PATCH_URL")
        if override_url:
            return self._post_json(
                override_url,
                {"message_id": message_id, "token": update_token, "card": card},
                headers={"Authorization": f"Bearer {token}"},
            )

        if update_token:
            return self._post_json(
                "https://open.feishu.cn/open-apis/interactive/v1/card/update",
                {"token": update_token, "card": card},
                headers={"Authorization": f"Bearer {token}"},
            )

        if message_id:
            return self._post_json(
                f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}",
                {
                    "msg_type": "interactive",
                    "content": json.dumps(card, ensure_ascii=False),
                },
                headers={"Authorization": f"Bearer {token}"},
                method="PATCH",
            )

        return {"ok": True, "dry_run": True, "payload": dry_payload}

    def send_webhook_text(self, text: str) -> Dict[str, Any]:
        timestamp = str(int(time.time()))
        payload: Dict[str, Any] = {"msg_type": "text", "content": {"text": text}}
        if self.webhook_secret:
            payload["timestamp"] = timestamp
            payload["sign"] = self._sign_custom_bot(timestamp, self.webhook_secret)
        if self.dry_run or not self.webhook_url:
            return {"ok": True, "dry_run": True, "payload": payload}
        result = self._post_json(self.webhook_url, payload)
        result["dry_run"] = False
        return result

    def _get_tenant_access_token(self) -> Optional[str]:
        if not (self.app_id and self.app_secret):
            return None
        if self._tenant_token and self._tenant_token[1] > time.time() + 60:
            return self._tenant_token[0]
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        result = self._post_json(
            url, {"app_id": self.app_id, "app_secret": self.app_secret}
        )
        if not result.get("ok"):
            return None
        body = result.get("body", {})
        token = body.get("tenant_access_token")
        expire = int(body.get("expire", 7200))
        if token:
            self._tenant_token = (token, time.time() + expire)
        return token

    def send_chat_text(self, chat_id: str, text: str) -> Dict[str, Any]:
        if self.dry_run or not (self.app_id and self.app_secret):
            return {
                "ok": True,
                "dry_run": True,
                "payload": {"chat_id": chat_id, "text": text},
            }
        token = self._get_tenant_access_token()
        if not token:
            return {"ok": False, "dry_run": False, "body": "tenant_access_token unavailable"}
        url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
        payload = {
            "receive_id": chat_id,
            "msg_type": "text",
            "content": json.dumps({"text": text}, ensure_ascii=False),
        }
        result = self._post_json(
            url, payload, headers={"Authorization": f"Bearer {token}"}
        )
        result["dry_run"] = False
        return result


def parse_feishu_event(payload: Dict[str, Any]) -> Dict[str, Any]:
    if "challenge" in payload:
        return {"kind": "challenge", "challenge": payload["challenge"]}

    card_action = parse_card_action(payload)
    if card_action:
        card_action["kind"] = "card_action"
        return card_action

    event = payload.get("event", {})
    header = payload.get("header", {})
    message = event.get("message", {})
    content = message.get("content", "")
    text = ""
    if isinstance(content, str):
        try:
            text = json.loads(content).get("text", content)
        except json.JSONDecodeError:
            text = content
    elif isinstance(content, dict):
        text = content.get("text", "")

    return {
        "kind": "message",
        "event_type": header.get("event_type") or payload.get("type"),
        "chat_id": message.get("chat_id"),
        "message_id": message.get("message_id"),
        "text": text.strip(),
    }


def handle_feishu_event(
    payload: Dict[str, Any], notifier: Optional[FeishuNotifier] = None
) -> Dict[str, Any]:
    parsed = parse_feishu_event(payload)
    if parsed["kind"] == "challenge":
        return {"challenge": parsed["challenge"]}
    if parsed["kind"] == "card_action":
        return handle_card_action(payload, notifier)

    answer = answer_question(parsed.get("text", ""))
    notifier = notifier or FeishuNotifier()
    send_result = None
    if parsed.get("chat_id"):
        send_result = notifier.send_chat_text(parsed["chat_id"], answer)

    return {
        "ok": True,
        "received": parsed,
        "answer": answer,
        "send_result": send_result,
    }


def simulate_daily_run(area: str = "Europe/*") -> Dict[str, Any]:
    snapshot = build_monitor_snapshot()
    report = build_daily_report(snapshot)
    return {
        "ok": True,
        "area": area,
        "pipeline": [
            "数据接入",
            "潜在削峰识别",
            "削峰场景判定",
            "双模型还原",
            "Kafka 生产下发",
            "日终分析",
            "效果回流与模型版本迭代闭环",
        ],
        "snapshot": snapshot,
        "report": report,
    }
