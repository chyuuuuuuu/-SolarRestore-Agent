"""Read-only Kubernetes Dashboard log access for the PV clipping demo."""

from __future__ import annotations

import http.cookiejar
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_DASHBOARD_URL = "https://your-k8s-dashboard.example.com"
DEFAULT_NAMESPACE = "data-platform"
DEFAULT_KEYWORDS = ("sigen-pv-clipping",)
LOG_END_POSITION = 2_000_000_000


class K8sDashboardError(RuntimeError):
    """Raised when the Dashboard API cannot be reached or authenticated."""


@dataclass
class K8sLogSource:
    pod: str
    namespace: str
    container: Optional[str]
    line_count: int
    ok: bool
    error: Optional[str] = None


class KubernetesDashboardClient:
    """Minimal client for Kubernetes Dashboard v2.x read-only APIs.

    The Dashboard first exchanges a Kubernetes bearer token for a jweToken.
    We keep both values in memory only and never write them to disk.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        timeout: float = 12.0,
    ) -> None:
        self.base_url = (base_url or os.getenv("PV_CLIPPING_K8S_DASHBOARD_URL") or DEFAULT_DASHBOARD_URL).rstrip("/")
        self.token = (
            token
            or os.getenv("PV_CLIPPING_K8S_TOKEN")
            or os.getenv("PV_CLIPPING_K8S_DASHBOARD_TOKEN")
            or ""
        ).strip()
        self.timeout = timeout
        self._jwe_token = ""
        self._login_at = 0.0
        self._cookies = http.cookiejar.CookieJar()
        self._opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self._cookies))

    def configured(self) -> bool:
        return bool(self.token)

    def _url(self, path: str, params: Optional[Dict[str, Any]] = None) -> str:
        url = f"{self.base_url}/{path.lstrip('/')}"
        if params:
            clean = {
                key: str(value)
                for key, value in params.items()
                if value is not None and str(value) != ""
            }
            if clean:
                url = f"{url}?{urllib.parse.urlencode(clean)}"
        return url

    def _request(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        authenticated: bool = True,
    ) -> Any:
        payload = None
        request_headers = {"Accept": "application/json"}
        if body is not None:
            payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
            request_headers["Content-Type"] = "application/json"
        if authenticated:
            self._ensure_login()
            request_headers["jweToken"] = self._jwe_token
        if headers:
            request_headers.update(headers)

        req = urllib.request.Request(
            self._url(path, params),
            data=payload,
            headers=request_headers,
            method=method.upper(),
        )
        try:
            with self._opener.open(req, timeout=self.timeout) as resp:
                raw = resp.read()
                content_type = resp.headers.get("Content-Type", "")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")
            raise K8sDashboardError(f"Dashboard API HTTP {exc.code}: {detail[:500]}") from exc
        except urllib.error.URLError as exc:
            raise K8sDashboardError(f"Dashboard API network error: {exc.reason}") from exc

        text = raw.decode("utf-8", "replace")
        if "json" in content_type or text.startswith(("{", "[")):
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                pass
        return text

    def _ensure_login(self) -> None:
        if self._jwe_token and time.time() - self._login_at < 20 * 60:
            return
        if not self.token:
            raise K8sDashboardError(
                "未配置 K8s token。请在运行 demo 服务前设置 PV_CLIPPING_K8S_TOKEN。"
            )
        csrf = self._request("GET", "api/v1/csrftoken/login", authenticated=False)
        csrf_token = csrf.get("token") if isinstance(csrf, dict) else None
        if not csrf_token:
            raise K8sDashboardError("Dashboard 未返回 CSRF token。")
        result = self._request(
            "POST",
            "api/v1/login",
            body={"token": self.token},
            headers={"X-CSRF-TOKEN": csrf_token},
            authenticated=False,
        )
        if not isinstance(result, dict):
            raise K8sDashboardError("Dashboard 登录返回非 JSON 响应。")
        errors = result.get("errors") or []
        if errors:
            raise K8sDashboardError(f"Dashboard 登录失败：{errors}")
        jwe_token = result.get("jweToken") or ""
        if not jwe_token:
            raise K8sDashboardError("Dashboard 登录成功但未返回 jweToken。")
        self._jwe_token = jwe_token
        self._login_at = time.time()

    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("GET", path, params=params)

    def list_pods(
        self,
        namespace: str = DEFAULT_NAMESPACE,
        keywords: Sequence[str] = DEFAULT_KEYWORDS,
        limit: int = 50,
    ) -> Dict[str, Any]:
        params = {
            "itemsPerPage": max(limit, 50),
            "page": 1,
            "sortBy": "d,creationTimestamp",
        }
        data = self.get_json(f"api/v1/pod/{namespace}", params=params)
        items = _extract_items(data)
        matched = [
            _compact_pod(item, namespace)
            for item in items
            if _matches_keywords(item, keywords)
        ]
        return {
            "ok": True,
            "source": "k8s_dashboard",
            "namespace": namespace,
            "keywords": list(keywords),
            "total_items": _extract_total(data, len(items)),
            "matched_count": len(matched),
            "pods": matched[:limit],
        }

    def list_log_containers(self, pod: str, namespace: str = DEFAULT_NAMESPACE) -> List[str]:
        """Return container names known by the Dashboard log source API."""
        errors: List[str] = []
        for resource_type in ("pod", "Pod"):
            try:
                data = self.get_json(f"api/v1/log/source/{namespace}/{pod}/{resource_type}")
            except K8sDashboardError as exc:
                errors.append(str(exc))
                continue
            if not isinstance(data, dict):
                continue
            names: List[str] = []
            for key in ("containerNames", "initContainerNames"):
                value = data.get(key)
                if isinstance(value, list):
                    names.extend(str(item) for item in value if item)
            return sorted(set(names))
        return []

    def read_log(
        self,
        pod: str,
        namespace: str = DEFAULT_NAMESPACE,
        container: Optional[str] = None,
        tail_lines: int = 500,
        previous: bool = False,
    ) -> str:
        if not container:
            containers = self.list_log_containers(pod=pod, namespace=namespace)
            container = containers[0] if containers else None
        if not container:
            raise K8sDashboardError(f"Pod {namespace}/{pod} 未找到可读取日志的 container。")

        window = max(1, min(int(tail_lines or 500), 2000))
        params = {
            "logFilePosition": "end",
            "referenceTimestamp": "newest",
            "referenceLineNum": 0,
            "offsetFrom": LOG_END_POSITION,
            "offsetTo": LOG_END_POSITION + window,
            "previous": str(previous).lower(),
        }
        data = self.get_json(f"api/v1/log/{namespace}/{pod}/{container}", params=params)
        return _normalize_log_payload(data, tail_lines=tail_lines)


def _extract_items(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if not isinstance(data, dict):
        return []
    for key in ("items", "pods", "list", "resources"):
        value = data.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    for value in data.values():
        if isinstance(value, list) and value and isinstance(value[0], dict):
            return value
    return []


def _extract_total(data: Any, fallback: int) -> int:
    if not isinstance(data, dict):
        return fallback
    meta = data.get("listMeta") or data.get("metadata") or {}
    for source in (data, meta):
        for key in ("totalItems", "total", "count"):
            value = source.get(key) if isinstance(source, dict) else None
            if isinstance(value, int):
                return value
    return fallback


def _pod_text(item: Dict[str, Any]) -> str:
    parts: List[str] = []
    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for nested in value.values():
                walk(nested)
        elif isinstance(value, list):
            for nested in value:
                walk(nested)
        elif isinstance(value, (str, int)):
            parts.append(str(value))
    walk(item)
    return " ".join(parts).lower()


def _matches_keywords(item: Dict[str, Any], keywords: Sequence[str]) -> bool:
    terms = [term.strip().lower() for term in keywords if term.strip()]
    if not terms:
        return True
    text = _pod_text(item)
    return any(term in text for term in terms)


def _containers_from_pod(item: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    for key in ("containers", "initContainers"):
        value = item.get(key)
        if isinstance(value, list):
            for container in value:
                if isinstance(container, dict) and container.get("name"):
                    names.append(str(container["name"]))
                elif isinstance(container, str):
                    names.append(container)
    pod_status = item.get("podStatus") or {}
    if isinstance(pod_status, dict):
        for value in pod_status.values():
            if isinstance(value, list):
                for container in value:
                    if isinstance(container, dict) and container.get("name"):
                        names.append(str(container["name"]))
    meta = item.get("objectMeta") or item.get("metadata") or {}
    labels = meta.get("labels") or {}
    if isinstance(labels, dict):
        for key in ("name", "app", "app.kubernetes.io/name"):
            if labels.get(key):
                names.append(str(labels[key]))
    return sorted(set(names))


def _compact_pod(item: Dict[str, Any], default_namespace: str) -> Dict[str, Any]:
    meta = item.get("objectMeta") or item.get("metadata") or {}
    name = meta.get("name") or item.get("name") or ""
    namespace = meta.get("namespace") or item.get("namespace") or default_namespace
    return {
        "name": name,
        "namespace": namespace,
        "status": item.get("status") or item.get("podStatus", {}).get("status") or "",
        "created": meta.get("creationTimestamp") or item.get("creationTimestamp"),
        "restarts": item.get("restartCount") or item.get("restarts") or 0,
        "containers": _containers_from_pod(item),
        "labels": meta.get("labels") or {},
    }


def _normalize_log_payload(data: Any, tail_lines: int = 500) -> str:
    if isinstance(data, str):
        lines = data.splitlines()
        return "\n".join(lines[-tail_lines:])
    if isinstance(data, dict):
        for key in ("logs", "log", "content", "message"):
            value = data.get(key)
            if isinstance(value, str):
                lines = value.splitlines()
                return "\n".join(lines[-tail_lines:])
            if isinstance(value, list):
                return _normalize_log_lines(value, tail_lines)
        return _normalize_log_lines(data.get("items") or [], tail_lines)
    if isinstance(data, list):
        return _normalize_log_lines(data, tail_lines)
    return ""


def _normalize_log_lines(items: Iterable[Any], tail_lines: int) -> str:
    lines: List[str] = []
    for item in items:
        if isinstance(item, dict):
            timestamp = item.get("timestamp") or item.get("time") or ""
            content = item.get("content") or item.get("log") or item.get("message") or ""
            line = f"{timestamp} {content}".strip()
        else:
            line = str(item)
        if line:
            lines.append(line)
    return "\n".join(lines[-tail_lines:])


ISSUE_PATTERNS: Tuple[Tuple[str, str], ...] = (
    ("功率数据插值失败", r"功率数据插值失败|power.*interpolat|interpolat.*power"),
    ("重复索引导致插值失败", r"重复索引|duplicate.*index|duplicate.*label|cannot reindex"),
    ("col 未定义历史 bug 风险", r"\bcol\b.*not defined|col 未定义"),
    ("无功率数据", r"无功率数据|no power data"),
    ("气象空表", r"Empty data passed with indices specified|气象空表|weather.*empty|empty.*weather"),
    ("clipping mask 计算失败", r"clipping mask.*失败|mask.*计算失败|mask.*failed"),
    ("too many values to unpack", r"too many values to unpack"),
    ("NoneType empty", r"NoneType.*empty|'NoneType' object has no attribute 'empty'"),
    ("接口/契约异常", r"contract|接口|返回值|unpack|TypeError|ValueError"),
)


def analyze_log_text(text: str, sample_limit: int = 20) -> Dict[str, Any]:
    lines = text.splitlines()
    severity = Counter()
    issues = Counter()
    samples: List[str] = []
    areas = Counter()
    summary = {
        "potential_clipping_stations": None,
        "success_stations": None,
        "failed_or_skipped_stations": None,
    }

    for line in lines:
        if "ERROR" in line:
            severity["ERROR"] += 1
        elif "WARNING" in line or "WARN" in line:
            severity["WARNING"] += 1
        elif "INFO" in line:
            severity["INFO"] += 1

        for name, pattern in ISSUE_PATTERNS:
            if re.search(pattern, line, re.IGNORECASE):
                issues[name] += 1
                if len(samples) < sample_limit:
                    samples.append(_trim_log_line(line))
                break

        area_match = re.search(r"([A-Za-z]+/[A-Za-z_+-]+)\s*区域", line)
        if area_match:
            areas[area_match.group(1)] += 1

        for key, pattern in (
            ("potential_clipping_stations", r"潜在削峰站点数[:：]\s*(\d+)"),
            ("success_stations", r"成功处理[:：]\s*(\d+)"),
            ("failed_or_skipped_stations", r"失败/跳过[:：]\s*(\d+)"),
        ):
            match = re.search(pattern, line)
            if match:
                summary[key] = int(match.group(1))

    total_issues = sum(issues.values())
    return {
        "line_count": len(lines),
        "severity_counts": dict(severity),
        "issue_counts": dict(issues.most_common()),
        "issue_total": total_issues,
        "areas": dict(areas.most_common(10)),
        "run_summary": summary,
        "samples": samples,
    }


def _trim_log_line(line: str, limit: int = 260) -> str:
    clean = re.sub(r"\s+", " ", line).strip()
    return clean if len(clean) <= limit else clean[: limit - 3] + "..."


def merge_log_analyses(analyses: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    severity = Counter()
    issues = Counter()
    areas = Counter()
    samples: List[str] = []
    line_count = 0
    run_summary: Dict[str, Optional[int]] = {
        "potential_clipping_stations": None,
        "success_stations": None,
        "failed_or_skipped_stations": None,
    }
    for analysis in analyses:
        line_count += int(analysis.get("line_count") or 0)
        severity.update(analysis.get("severity_counts") or {})
        issues.update(analysis.get("issue_counts") or {})
        areas.update(analysis.get("areas") or {})
        samples.extend(analysis.get("samples") or [])
        for key, value in (analysis.get("run_summary") or {}).items():
            if value is not None:
                run_summary[key] = value
    return {
        "line_count": line_count,
        "severity_counts": dict(severity),
        "issue_counts": dict(issues.most_common()),
        "issue_total": sum(issues.values()),
        "areas": dict(areas.most_common(10)),
        "run_summary": run_summary,
        "samples": samples[:20],
    }


def query_k8s_log_summary(
    namespace: str = DEFAULT_NAMESPACE,
    keywords: Sequence[str] = DEFAULT_KEYWORDS,
    limit_pods: int = 8,
    tail_lines: int = 500,
    container: Optional[str] = None,
    previous: bool = False,
) -> Dict[str, Any]:
    client = KubernetesDashboardClient()
    pod_result = client.list_pods(namespace=namespace, keywords=keywords, limit=max(limit_pods, 20))
    selected_pods = pod_result["pods"][:limit_pods]
    analyses: List[Dict[str, Any]] = []
    sources: List[Dict[str, Any]] = []

    for pod in selected_pods:
        pod_name = pod["name"]
        containers = [container] if container else (pod.get("containers") or [None])
        # Keep the demo bounded even if a Pod has many sidecars.
        for each_container in containers[:3]:
            try:
                log_text = client.read_log(
                    pod=pod_name,
                    namespace=pod.get("namespace") or namespace,
                    container=each_container,
                    tail_lines=tail_lines,
                    previous=previous,
                )
                analysis = analyze_log_text(log_text)
                analyses.append(analysis)
                sources.append(
                    K8sLogSource(
                        pod=pod_name,
                        namespace=pod.get("namespace") or namespace,
                        container=each_container,
                        line_count=analysis["line_count"],
                        ok=True,
                    ).__dict__
                )
            except Exception as exc:
                sources.append(
                    K8sLogSource(
                        pod=pod_name,
                        namespace=pod.get("namespace") or namespace,
                        container=each_container,
                        line_count=0,
                        ok=False,
                        error=f"{type(exc).__name__}: {exc}",
                    ).__dict__
                )

    merged = merge_log_analyses(analyses)
    return {
        "ok": True,
        "source": "k8s_dashboard",
        "namespace": namespace,
        "keywords": list(keywords),
        "tail_lines": tail_lines,
        "pod_total": pod_result["total_items"],
        "matched_pods": pod_result["matched_count"],
        "selected_pods": selected_pods,
        "log_sources": sources,
        "analysis": merged,
    }


def parse_keywords(raw: str) -> List[str]:
    terms = [term.strip() for term in re.split(r"[,，\s]+", raw or "") if term.strip()]
    return terms or list(DEFAULT_KEYWORDS)
