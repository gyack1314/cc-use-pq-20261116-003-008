from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from typing import Any

IDENTIFIER = re.compile(r"^[a-z][a-z0-9_-]{1,39}$")
METRIC = re.compile(r"^[a-z][a-z0-9_]{1,31}$")
ALLOWED_UNITS = {"C", "K", "mA", "A", "mAh", "Ah", "g", "kg", "percent", "unitless"}


class DomainError(ValueError):
    def __init__(self, code: str, detail: str):
        self.code = code
        super().__init__(detail)


def require_identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise DomainError("invalid_identifier", f"{label} must use 2-40 lowercase letters, digits, '_' or '-'")
    return value


def require_metric(value: Any) -> str:
    if not isinstance(value, str) or not METRIC.fullmatch(value):
        raise DomainError("invalid_metric", "metric name is invalid")
    return value


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def parse_time(value: Any) -> str:
    if not isinstance(value, str):
        raise DomainError("invalid_time", "timestamp must be ISO-8601 text")
    try:
        instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DomainError("invalid_time", "timestamp must be ISO-8601 text") from exc
    if instant.tzinfo is None:
        raise DomainError("invalid_time", "timestamp needs an explicit UTC offset")
    return instant.astimezone(timezone.utc).isoformat(timespec="microseconds")


def decimal(value: Any, label: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        raise DomainError("invalid_number", f"{label} must be a finite number")
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise DomainError("invalid_number", f"{label} must be a finite number") from exc
    if not number.is_finite():
        raise DomainError("invalid_number", f"{label} must be a finite number")
    return number


def integer(value: Any, label: str, low: int, high: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise DomainError("invalid_integer", f"{label} must be an integer in [{low}, {high}]")
    return value


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


@dataclass(frozen=True)
class MetricRule:
    name: str
    unit: str
    minimum: Decimal
    maximum: Decimal
    required: bool
    precision: int

    @classmethod
    def from_dict(cls, item: dict[str, Any]) -> MetricRule:
        if not isinstance(item, dict):
            raise DomainError("invalid_rule", "metric rule must be an object")
        name = require_metric(item.get("name"))
        unit = item.get("unit")
        if unit not in ALLOWED_UNITS:
            raise DomainError("invalid_unit", f"metric {name} has unsupported unit")
        minimum = decimal(item.get("minimum"), "minimum")
        maximum = decimal(item.get("maximum"), "maximum")
        if minimum >= maximum:
            raise DomainError("invalid_rule", f"metric {name} bounds are reversed")
        required = item.get("required", True)
        if not isinstance(required, bool):
            raise DomainError("invalid_rule", "required must be boolean")
        precision = integer(item.get("precision", 3), "precision", 0, 9)
        return cls(name, unit, minimum, maximum, required, precision)

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "unit": self.unit, "minimum": str(self.minimum),
                "maximum": str(self.maximum), "required": self.required, "precision": self.precision}

    def check(self, value: Any) -> str:
        number = decimal(value, self.name)
        if number < self.minimum or number > self.maximum:
            raise DomainError("out_of_range", f"{self.name} is outside declared bounds")
        if abs(number.as_tuple().exponent) > self.precision:
            raise DomainError("excess_precision", f"{self.name} exceeds declared precision")
        return str(number)


@dataclass(frozen=True)
class CampaignSpec:
    campaign_id: str
    version: int
    title: str
    groups: tuple[str, ...]
    rules: tuple[MetricRule, ...]
    min_samples: int
    max_bad_fraction: Decimal
    window_hours: int
    baseline: str

    @classmethod
    def from_dict(cls, item: dict[str, Any], version: int = 1) -> CampaignSpec:
        if not isinstance(item, dict):
            raise DomainError("invalid_spec", "campaign spec must be an object")
        campaign_id = require_identifier(item.get("campaign_id"), "campaign_id")
        title = item.get("title")
        if not isinstance(title, str) or not 3 <= len(title.strip()) <= 160:
            raise DomainError("invalid_spec", "title must have 3-160 characters")
        groups = item.get("groups")
        if not isinstance(groups, list) or not 2 <= len(groups) <= 12:
            raise DomainError("invalid_groups", "campaign needs 2-12 groups")
        group_ids = tuple(require_identifier(group, "group") for group in groups)
        if len(set(group_ids)) != len(group_ids):
            raise DomainError("invalid_groups", "group identifiers must be unique")
        baseline = item.get("baseline")
        if baseline not in group_ids:
            raise DomainError("invalid_baseline", "baseline must be a declared group")
        metrics = item.get("metrics")
        if not isinstance(metrics, list) or not 1 <= len(metrics) <= 24:
            raise DomainError("invalid_metrics", "campaign needs 1-24 metrics")
        rules = tuple(MetricRule.from_dict(metric) for metric in metrics)
        if len({rule.name for rule in rules}) != len(rules):
            raise DomainError("invalid_metrics", "metric names must be unique")
        samples = integer(item.get("min_samples", 2), "min_samples", 1, 100000)
        fraction = decimal(item.get("max_bad_fraction", "0.1"), "max_bad_fraction")
        if not Decimal("0") <= fraction < Decimal("1"):
            raise DomainError("invalid_fraction", "max_bad_fraction must be in [0, 1)")
        hours = integer(item.get("window_hours", 24), "window_hours", 1, 720)
        return cls(campaign_id, version, title.strip(), group_ids, rules, samples, fraction, hours, baseline)

    def to_dict(self) -> dict[str, Any]:
        return {"campaign_id": self.campaign_id, "version": self.version, "title": self.title,
                "groups": list(self.groups), "baseline": self.baseline,
                "metrics": [rule.to_dict() for rule in self.rules],
                "min_samples": self.min_samples, "max_bad_fraction": str(self.max_bad_fraction),
                "window_hours": self.window_hours}

    def validate_measurements(self, measurements: Any) -> dict[str, str]:
        if not isinstance(measurements, dict):
            raise DomainError("invalid_measurements", "measurements must be an object")
        if not all(isinstance(name, str) for name in measurements):
            raise DomainError("invalid_measurements", "measurement names must be strings")
        allowed = {rule.name for rule in self.rules}
        unknown = set(measurements) - allowed
        if unknown:
            raise DomainError("unknown_metric", f"undeclared metrics: {', '.join(sorted(unknown))}")
        normalized = {}
        for rule in self.rules:
            if rule.name not in measurements or measurements[rule.name] is None:
                if rule.required:
                    raise DomainError("missing_metric", f"required metric {rule.name} is missing")
                continue
            normalized[rule.name] = rule.check(measurements[rule.name])
        return normalized
