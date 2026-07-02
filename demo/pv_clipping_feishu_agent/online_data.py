"""Read-only online StarRocks queries for the PV clipping demo."""

from __future__ import annotations

import ast
import json
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pymysql
import yaml


class OnlineDataError(RuntimeError):
    pass


POINT_INTERVAL_HOURS = 5 / 60
PREDICTION_VISUAL_SCALE = 12.0


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _parse_sequence(raw: Any) -> List[Any]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return list(raw)
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "replace")
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        for parser in (json.loads, ast.literal_eval):
            try:
                parsed = parser(text)
                if isinstance(parsed, (list, tuple)):
                    return list(parsed)
            except Exception:
                pass
        return [part.strip() for part in text.strip("[]").split(",") if part.strip()]
    return []


def _float_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None


def _to_float_list(raw: Any) -> List[Optional[float]]:
    return [_float_or_none(item) for item in _parse_sequence(raw)]


def _to_int_list(raw: Any) -> List[int]:
    values: List[int] = []
    for item in _parse_sequence(raw):
        try:
            values.append(int(float(item)))
        except (TypeError, ValueError):
            continue
    return values


def _time_label(index: int) -> str:
    minute = index * 5
    return f"{minute // 60:02d}:{minute % 60:02d}"


def _sum_energy(values: Sequence[Optional[float]]) -> float:
    return round(sum(float(value) for value in values if value is not None) * POINT_INTERVAL_HOURS, 4)


def _max_numeric(values: Sequence[Optional[float]]) -> float:
    numbers = [value for value in values if value is not None]
    return max(numbers) if numbers else 0.0


def build_restoration_curve_payload(
    station_id: str,
    date_str: str,
    prediction_rows: Sequence[Dict[str, Any]],
    observed_rows: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    observed_by_time: Dict[int, Dict[str, Any]] = {}
    for row in observed_rows:
        try:
            timestamp = int(float(row.get("statistics_time")))
        except (TypeError, ValueError):
            continue
        observed_by_time[timestamp] = row

    curves: List[Dict[str, Any]] = []
    seen_models = set()
    for row in prediction_rows:
        model_name = str(row.get("model_name") or "unknown")
        if model_name in seen_models:
            continue
        times = _to_int_list(row.get("statistics_time"))
        restored_values = _to_float_list(row.get("predicted_value"))
        size = min(len(times), len(restored_values))
        if size == 0:
            continue
        seen_models.add(model_name)

        raw_after_values: List[Optional[float]] = []
        for idx in range(size):
            raw_after_values.append(restored_values[idx])

        raw_restored_peak = _max_numeric(raw_after_values)
        scale_factor = PREDICTION_VISUAL_SCALE
        scale_mode = "fixed_prediction_x12"

        points: List[Dict[str, Any]] = []
        before_values: List[Optional[float]] = []
        after_values: List[Optional[float]] = []
        gain_values: List[float] = []
        for idx in range(size):
            timestamp = times[idx]
            observed_row = observed_by_time.get(timestamp) or {}
            before = _float_or_none(observed_row.get("observed_power"))
            grid_power = _float_or_none(observed_row.get("grid_power"))
            load_power = _float_or_none(observed_row.get("load_power"))
            raw_after = restored_values[idx]
            after = raw_after * scale_factor if raw_after is not None else None
            diff = after - before if after is not None and before is not None else None
            positive_gain = max(diff or 0.0, 0.0)
            before_values.append(before)
            after_values.append(after)
            gain_values.append(positive_gain)
            points.append(
                {
                    "index": idx,
                    "timestamp": timestamp,
                    "time_label": _time_label(idx),
                    "observed_power": before,
                    "restored_power": after,
                    "restored_power_raw": raw_after,
                    "grid_power": grid_power,
                    "load_power": load_power,
                    "gain": round(diff, 5) if diff is not None else None,
                }
            )

        observed_numeric = [value for value in before_values if value is not None]
        restored_numeric = [value for value in after_values if value is not None]
        observed_peak = max(observed_numeric) if observed_numeric else 0.0
        restored_peak = max(restored_numeric) if restored_numeric else 0.0
        gain_energy = round(sum(gain_values) * POINT_INTERVAL_HOURS, 4)
        observed_energy = _sum_energy(before_values)
        restored_energy = _sum_energy(after_values)
        net_energy_delta = round(restored_energy - observed_energy, 4)
        threshold = max(0.01, observed_peak * 0.02)
        gain_points = sum(1 for value in gain_values if value > threshold)
        curves.append(
            {
                "model_name": model_name,
                "model_version": row.get("model_version"),
                "record_time": row.get("record_time"),
                "point_count": size,
                "points": points,
                "stats": {
                    "observed_peak": round(observed_peak, 4),
                    "restored_peak": round(restored_peak, 4),
                    "peak_lift": round(restored_peak - observed_peak, 4),
                    "observed_energy": observed_energy,
                    "restored_energy": restored_energy,
                    "restored_gain_energy": gain_energy,
                    "extra_generation_energy": gain_energy,
                    "net_energy_delta": net_energy_delta,
                    "gain_ratio": round(gain_energy / observed_energy * 100, 2) if observed_energy else 0.0,
                    "gain_points": gain_points,
                    "raw_restored_peak": round(raw_restored_peak, 6),
                    "scale_factor": round(scale_factor, 6),
                    "scale_mode": scale_mode,
                },
            }
        )

    return {
        "ok": True,
        "source": "starrocks_9030",
        "station_id": str(station_id),
        "date": date_str,
        "predict_type": 4,
        "unit": "source_power_divided_by_1000",
        "scale_note": "pv_prediction_history.predicted_value is multiplied by 12 for visualization and energy delta calculation.",
        "point_interval_minutes": 5,
        "observed_point_count": len(observed_by_time),
        "model_count": len(curves),
        "curves": curves,
        "selected_curve": curves[0] if curves else None,
    }


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def load_db_config(kind: str = "starrocks") -> Dict[str, Any]:
    """Load DB config from env first, then existing Nacos snapshot files."""
    prefix = "PV_CLIPPING_STARROCKS" if kind == "starrocks" else "PV_CLIPPING_MYSQL"
    env_conf = {
        "host": os.getenv(f"{prefix}_HOST"),
        "port": os.getenv(f"{prefix}_PORT"),
        "user": os.getenv(f"{prefix}_USER"),
        "password": os.getenv(f"{prefix}_PASSWORD"),
        "database": os.getenv(f"{prefix}_DATABASE"),
    }
    if env_conf["host"] and env_conf["user"] and env_conf["password"]:
        env_conf["port"] = int(env_conf["port"] or (9030 if kind == "starrocks" else 3306))
        env_conf["database"] = env_conf["database"] or "sigen_device"
        return env_conf

    root = _repo_root()
    candidates = [
        root / "src/nacos-data/snapshot/sigen-pv-clipping-model.yml+DEFAULT_GROUP+public",
        root / "src/nacos-data/snapshot/sigen-pv-prediction-model.yml+DEFAULT_GROUP+public",
        root / "src/nacos-data/snapshot/sigen-pv-prediction-test.yaml+DEFAULT_GROUP+public",
    ]
    for path in candidates:
        data = _read_yaml(path)
        conf = data.get(kind) if isinstance(data, dict) else None
        if conf and conf.get("host") and conf.get("user") and conf.get("password"):
            return {
                "host": conf["host"],
                "port": int(conf.get("port") or (9030 if kind == "starrocks" else 3306)),
                "user": conf["user"],
                "password": conf["password"],
                "database": conf.get("database") or "sigen_device",
            }
    raise OnlineDataError(f"{kind} config not found")


class OnlinePVClippingRepository:
    def __init__(self, starrocks_config: Optional[Dict[str, Any]] = None) -> None:
        self.starrocks_config = starrocks_config or load_db_config("starrocks")

    def _connect_starrocks(self):
        conf = self.starrocks_config
        return pymysql.connect(
            host=conf["host"],
            port=int(conf.get("port", 9030)),
            user=conf["user"],
            password=conf["password"],
            database=conf.get("database", "sigen_device"),
            charset="utf8mb4",
            connect_timeout=8,
            read_timeout=20,
            write_timeout=20,
        )

    def _fetchall(self, sql: str, params: Sequence[Any]) -> List[Dict[str, Any]]:
        conn = self._connect_starrocks()
        try:
            with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(sql, params)
                return [_json_safe(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    def _fetchone(self, sql: str, params: Sequence[Any]) -> Dict[str, Any]:
        rows = self._fetchall(sql, params)
        return rows[0] if rows else {}

    def clipping_summary(
        self,
        date_str: str,
        station_id: Optional[str] = None,
        model_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        where = ["dt = %s", "predict_type = 4"]
        params: List[Any] = [date_str]
        if station_id:
            where.append("station_id = %s")
            params.append(str(station_id))
        if model_name:
            where.append("model_name = %s")
            params.append(model_name)
        where_sql = " AND ".join(where)

        summary_sql = f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT station_id) AS restored_station_count,
                COUNT(DISTINCT model_name) AS model_count,
                MIN(record_time) AS first_record_time,
                MAX(record_time) AS last_record_time
            FROM sigen_device.pv_prediction_history
            WHERE {where_sql}
        """
        model_sql = f"""
            SELECT
                model_name,
                COUNT(*) AS row_count,
                COUNT(DISTINCT station_id) AS restored_station_count,
                MIN(record_time) AS first_record_time,
                MAX(record_time) AS last_record_time
            FROM sigen_device.pv_prediction_history
            WHERE {where_sql}
            GROUP BY model_name
            ORDER BY restored_station_count DESC, row_count DESC
        """
        station_sql = f"""
            SELECT
                station_id,
                COUNT(*) AS row_count,
                COUNT(DISTINCT model_name) AS model_count,
                MIN(record_time) AS first_record_time,
                MAX(record_time) AS last_record_time
            FROM sigen_device.pv_prediction_history
            WHERE {where_sql}
            GROUP BY station_id
            ORDER BY row_count DESC
            LIMIT 20
        """

        summary = self._fetchone(summary_sql, params)
        return {
            "ok": True,
            "source": "starrocks_9030",
            "date": date_str,
            "predict_type": 4,
            "station_id": station_id,
            "model_name": model_name,
            "row_count": int(summary.get("row_count") or 0),
            "restored_station_count": int(summary.get("restored_station_count") or 0),
            "model_count": int(summary.get("model_count") or 0),
            "first_record_time": summary.get("first_record_time"),
            "last_record_time": summary.get("last_record_time"),
            "model_breakdown": self._fetchall(model_sql, params),
            "top_stations": self._fetchall(station_sql, params),
            "sql_template": (
                "SELECT COUNT(DISTINCT station_id) "
                "FROM sigen_device.pv_prediction_history "
                "WHERE dt = ? AND predict_type = 4"
            ),
        }

    def station_prediction_history(
        self,
        station_id: str,
        date_str: str,
        limit: int = 20,
        include_payload: bool = False,
    ) -> Dict[str, Any]:
        limit = max(1, min(int(limit), 100))
        fields = "*"
        if not include_payload:
            fields = "dt, station_id, predict_type, model_name, model_version, record_time"
        sql = f"""
            SELECT {fields}
            FROM sigen_device.pv_prediction_history
            WHERE station_id = %s
              AND dt = %s
              AND predict_type = 4
            ORDER BY record_time ASC
            LIMIT {limit}
        """
        rows = self._fetchall(sql, [str(station_id), date_str])
        return {
            "ok": True,
            "source": "starrocks_9030",
            "station_id": str(station_id),
            "date": date_str,
            "predict_type": 4,
            "include_payload": include_payload,
            "row_count": len(rows),
            "rows": rows,
        }

    def station_observed_power_curve(self, station_id: str, date_str: str) -> List[Dict[str, Any]]:
        sql = """
            SELECT
                statistics_time,
                filtered_pv_total_power / 1000 AS observed_power,
                filtered_pv_to_grid_power / 1000 AS grid_power,
                filtered_load_power / 1000 AS load_power,
                bat_soc / 10 AS bat_soc
            FROM sigen_device.station_statistics_min
            WHERE station_id = %s
              AND dt = %s
            ORDER BY statistics_time ASC
        """
        return self._fetchall(sql, [str(station_id), date_str])

    def station_restoration_curve(
        self,
        station_id: str,
        date_str: str,
        model_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        where = ["station_id = %s", "dt = %s", "predict_type = 4"]
        params: List[Any] = [str(station_id), date_str]
        if model_name:
            where.append("model_name = %s")
            params.append(model_name)
        where_sql = " AND ".join(where)
        prediction_sql = f"""
            SELECT
                dt,
                station_id,
                predict_type,
                model_name,
                model_version,
                record_time,
                statistics_time,
                predicted_value
            FROM sigen_device.pv_prediction_history
            WHERE {where_sql}
            ORDER BY record_time DESC
            LIMIT 12
        """
        prediction_rows = self._fetchall(prediction_sql, params)
        observed_rows = self.station_observed_power_curve(station_id=station_id, date_str=date_str)
        payload = build_restoration_curve_payload(
            station_id=station_id,
            date_str=date_str,
            prediction_rows=prediction_rows,
            observed_rows=observed_rows,
        )
        payload["model_name"] = model_name
        return payload


def query_online_clipping_summary(
    date_str: str,
    station_id: Optional[str] = None,
    model_name: Optional[str] = None,
) -> Dict[str, Any]:
    repo = OnlinePVClippingRepository()
    return repo.clipping_summary(date_str, station_id=station_id, model_name=model_name)


def query_online_station_history(
    station_id: str,
    date_str: str,
    limit: int = 20,
    include_payload: bool = False,
) -> Dict[str, Any]:
    repo = OnlinePVClippingRepository()
    return repo.station_prediction_history(
        station_id=station_id,
        date_str=date_str,
        limit=limit,
        include_payload=include_payload,
    )


def query_online_restoration_curve(
    station_id: str,
    date_str: str,
    model_name: Optional[str] = None,
) -> Dict[str, Any]:
    repo = OnlinePVClippingRepository()
    return repo.station_restoration_curve(
        station_id=station_id,
        date_str=date_str,
        model_name=model_name,
    )
