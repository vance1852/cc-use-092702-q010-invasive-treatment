"""处置方案版本的领域规则。

方案是不可覆盖的：每次“生成”都得到一个带序号和内容哈希的新版本。
方案内容在生成时由当时有效的风险记录与影响范围快照决定，
之后任何执行与查询都以 ``plan_version`` 为依据。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from .clock import parse_utc

# 主流程步骤键（严格先后顺序）
STEP_KEYS: tuple[str, ...] = (
    "notice",          # 封控通知签收
    "isolation",       # 现场隔离
    "clearing",        # 分区清除
    "reinspection",    # 复查样方
    "disposal",        # 废弃物去向
    "review",          # 独立复核结案
)

STEP_LABELS: dict[str, str] = {
    "notice": "封控通知签收",
    "isolation": "现场隔离",
    "clearing": "分区清除",
    "reinspection": "复查样方",
    "disposal": "废弃物转运处置",
    "review": "独立复核结案",
}

# 返工步骤键，位于复查与废弃物处置之间
REWORK_KEY: str = "rework"
REWORK_LABEL: str = "返工清除"

# 各步骤相对时间锚点的时限（小时）。锚点：
# notice/isolation/clearing 自方案版本生效起；
# reinspection 自末次清除完成起；disposal 自复查通过起；review 自废弃物处置完成起。
DEFAULT_DEADLINE_HOURS: dict[str, float] = {
    "notice": 12,
    "isolation": 24,
    "clearing": 72,
    "reinspection": 168,
    "disposal": 48,
    "review": 24,
    "rework": 24,
}

CANONICAL_STEP_KEYS: tuple[str, ...] = STEP_KEYS[:4] + (REWORK_KEY,) + STEP_KEYS[4:]


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def content_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class AffectedArea:
    """影响范围（林缘成片发生区）。"""
    area_id: str
    zone_record_id: str
    location: str
    area_m2: float
    polygon: tuple[str, ...] = field(default_factory=tuple)
    observed_at: str = ""

    def validate(self) -> None:
        if not self.area_id.strip() or not self.zone_record_id.strip():
            raise ValueError("area_id 与 zone_record_id 必填")
        if self.area_m2 <= 0:
            raise ValueError("area_m2 必须为正数")
        parse_utc(self.observed_at, "observed_at")

    def snapshot(self) -> dict[str, Any]:
        return {
            "area_id": self.area_id,
            "zone_record_id": self.zone_record_id,
            "location": self.location,
            "area_m2": self.area_m2,
            "polygon": sorted(self.polygon),
            "observed_at": self.observed_at,
        }


@dataclass(frozen=True)
class ClearingZone:
    """分区清除中的一个清除分区。"""
    zone_code: str
    method: str
    area_m2: float

    def validate(self) -> None:
        if not self.zone_code.strip() or not self.method.strip():
            raise ValueError("清除分区编号与方法必填")
        if self.area_m2 <= 0:
            raise ValueError("清除分区面积必须为正数")

    def snapshot(self) -> dict[str, Any]:
        return {"zone_code": self.zone_code, "method": self.method, "area_m2": self.area_m2}


def _normalize_deadlines(raw: Mapping[str, float] | None) -> dict[str, float]:
    deadlines = dict(DEFAULT_DEADLINE_HOURS)
    for key, value in (raw or {}).items():
        if key not in CANONICAL_STEP_KEYS:
            raise ValueError(f"未知步骤时限: {key}")
        hours = float(value)
        if hours <= 0:
            raise ValueError(f"步骤 {key} 的时限必须为正数")
        deadlines[key] = hours
    return deadlines


def build_plan_content(
    *,
    risk_snapshot: Mapping[str, Any],
    area: AffectedArea,
    clearing_zones: Sequence[ClearingZone],
    notice_recipients: Sequence[str],
    chemical_batches: Sequence[str],
    reason: str,
    deadline_hours: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    """由当时有效的风险记录与影响范围构造规范化方案内容。"""
    area.validate()
    zones = tuple(clearing_zones)
    if not zones:
        raise ValueError("至少定义一个清除分区")
    for item in zones:
        item.validate()
    recipients = sorted({item.strip() for item in notice_recipients if item.strip()})
    if not recipients:
        raise ValueError("至少一个封控通知接收方")
    batches = sorted({item.strip() for item in chemical_batches if item.strip()})
    if not batches:
        raise ValueError("至少登记一个药剂批次")
    if not reason.strip():
        raise ValueError("生成方案必须说明变更原因")
    risk = dict(risk_snapshot)
    for required in ("alert_id", "severity", "score", "status", "captured_at"):
        if required not in risk:
            raise ValueError(f"风险快照缺少 {required}")
    parse_utc(risk["captured_at"], "risk.captured_at")
    deadlines = _normalize_deadlines(deadline_hours)
    return {
        "risk": {
            "alert_id": risk["alert_id"],
            "zone_record_id": risk.get("zone_record_id", area.zone_record_id),
            "severity": risk["severity"],
            "score": risk["score"],
            "status": risk["status"],
            "captured_at": risk["captured_at"],
        },
        "area": area.snapshot(),
        "clearing_zones": sorted((item.snapshot() for item in zones), key=lambda item: item["zone_code"]),
        "notice_recipients": recipients,
        "chemical_batches": batches,
        "deadline_hours": {key: deadlines[key] for key in CANONICAL_STEP_KEYS},
        "reason": reason.strip(),
    }


def steps_for_version(content: Mapping[str, Any], rework_round: int) -> list[dict[str, Any]]:
    """返回某方案版本某一轮返工下应执行的步骤序列。"""
    deadlines = content["deadline_hours"]
    steps: list[dict[str, Any]] = []
    for key in STEP_KEYS[:4]:
        steps.append({"step_key": key, "label": STEP_LABELS[key], "deadline_hours": deadlines[key]})
    if rework_round > 0:
        steps.append({
            "step_key": REWORK_KEY,
            "label": f"{REWORK_LABEL}（第 {rework_round} 轮）",
            "deadline_hours": deadlines[REWORK_KEY],
            "rework_round": rework_round,
        })
    for key in STEP_KEYS[4:]:
        steps.append({"step_key": key, "label": STEP_LABELS[key], "deadline_hours": deadlines[key]})
    return steps
