from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from .domain import CampaignSpec, DomainError, decimal, parse_time


def window_bounds(anchor: str, hours: int) -> tuple[str, str]:
    instant = datetime.fromisoformat(parse_time(anchor))
    seconds = hours * 3600
    epoch = int(instant.timestamp())
    start = datetime.fromtimestamp(epoch - epoch % seconds, tz=instant.tzinfo)
    return start.isoformat(timespec="microseconds"), (start + timedelta(hours=hours)).isoformat(timespec="microseconds")


def summarize_quality(spec: CampaignSpec, samples: list[dict[str, Any]],
                      rejections: list[dict[str, Any]], start: str, end: str) -> dict[str, Any]:
    by_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for sample in samples:
        if sample["status"] == "active":
            by_group[sample["group_id"]].append(sample)
    total = sum(len(group) for group in by_group.values())
    rejected = len(rejections)
    denominator = total + rejected
    bad_fraction = Decimal(rejected) / Decimal(denominator) if denominator else Decimal("0")
    reasons = Counter(item["reason"] for item in rejections)
    gates = []
    if total == 0:
        gates.append("no_samples")
    if bad_fraction > spec.max_bad_fraction:
        gates.append("excess_rejection_fraction")
    for group in spec.groups:
        if len(by_group[group]) < spec.min_samples:
            gates.append(f"insufficient_samples:{group}")
    for rule in spec.rules:
        if not rule.required:
            continue
        missing = sum(rule.name not in sample["measurements"] for sample in samples if sample["status"] == "active")
        if missing:
            gates.append(f"missing_required_metric:{rule.name}")
    return {"window": {"start": start, "end": end}, "accepted_count": total,
            "rejected_count": rejected, "bad_fraction": str(bad_fraction),
            "group_counts": {group: len(by_group[group]) for group in spec.groups},
            "rejection_reasons": dict(sorted(reasons.items())), "gates": gates,
            "passed": not gates}


def qualified_rejections(rejections: list[dict[str, Any]], start: str,
                         end: str) -> list[dict[str, Any]]:
    return [item for item in rejections if item.get("event_time") and start <= item["event_time"] < end]


def validate_window(spec: CampaignSpec, start: Any, end: Any) -> tuple[str, str]:
    left, right = parse_time(start), parse_time(end)
    if left >= right:
        raise DomainError("invalid_window", "window start must precede end")
    length = datetime.fromisoformat(right) - datetime.fromisoformat(left)
    if length > timedelta(hours=spec.window_hours):
        raise DomainError("invalid_window", "window exceeds campaign window_hours")
    return left, right


def metric_health(spec: CampaignSpec, samples: list[dict[str, Any]]) -> dict[str, Any]:
    result = {}
    for rule in spec.rules:
        covered = sum(rule.name in sample["measurements"] for sample in samples)
        result[rule.name] = {"covered": covered, "missing": len(samples) - covered,
                             "coverage": str(Decimal(covered) / Decimal(len(samples))) if samples else "0"}
    return result


def rejection_report(rejections: list[dict[str, Any]]) -> dict[str, Any]:
    by_reason = Counter(item["reason"] for item in rejections)
    by_batch = Counter(item["batch_id"] for item in rejections)
    return {"total": len(rejections), "by_reason": dict(sorted(by_reason.items())),
            "by_batch": dict(sorted(by_batch.items()))}


def compare_gate(spec: CampaignSpec, first: dict[str, Any], second: dict[str, Any]) -> dict[str, Any]:
    first_counts = first.get("group_counts", {})
    second_counts = second.get("group_counts", {})
    delta = {group: second_counts.get(group, 0) - first_counts.get(group, 0) for group in spec.groups}
    return {"count_delta": delta, "passed_before": bool(first.get("passed")),
            "passed_after": bool(second.get("passed")),
            "gate_added": sorted(set(second.get("gates", [])) - set(first.get("gates", []))),
            "gate_removed": sorted(set(first.get("gates", [])) - set(second.get("gates", [])))}


def validate_rejection_budget(value: Any) -> Decimal:
    fraction = decimal(value, "rejection budget")
    if not Decimal("0") <= fraction < Decimal("1"):
        raise DomainError("invalid_fraction", "rejection budget must be in [0, 1)")
    return fraction
