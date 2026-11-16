from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, localcontext
from typing import Any

from .analysis import statistics
from .domain import DomainError, require_identifier, require_metric
from .quality import validate_window
from .store import Store


def percentile(values: list[Decimal], fraction: Decimal) -> str | None:
    if not values:
        return None
    ordered = sorted(values)
    position = fraction * Decimal(len(values) - 1)
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    weight = position - Decimal(lower)
    with localcontext() as context:
        context.prec = 32
        return str(ordered[lower] * (1 - weight) + ordered[upper] * weight)


class Explorer:
    def __init__(self, store: Store):
        self.store = store

    def distribution(self, campaign_id: str, metric: str,
                     start: str, end: str) -> dict[str, Any]:
        require_identifier(campaign_id, "campaign_id")
        require_metric(metric)
        with self.store.connect() as db:
            spec = self.store.get_spec(campaign_id, db=db)
            if metric not in {rule.name for rule in spec.rules}:
                raise DomainError("unknown_metric", "metric is not declared")
            left, right = validate_window(spec, start, end)
            samples = self.store.list_samples(campaign_id, status="active", start=left, end=right, db=db)
            groups: dict[str, list[Decimal]] = defaultdict(list)
            missing = defaultdict(int)
            for sample in samples:
                if metric not in sample["measurements"]:
                    missing[sample["group_id"]] += 1
                else:
                    groups[sample["group_id"]].append(Decimal(sample["measurements"][metric]))
            result = {}
            for group in spec.groups:
                values = groups[group]
                result[group] = {"statistics": statistics(values),
                                 "p10": percentile(values, Decimal("0.1")),
                                 "p25": percentile(values, Decimal("0.25")),
                                 "p75": percentile(values, Decimal("0.75")),
                                 "p90": percentile(values, Decimal("0.9")),
                                 "missing": missing[group]}
            return {"campaign_id": campaign_id, "metric": metric,
                    "window": {"start": left, "end": right}, "groups": result}

    def paired_metrics(self, campaign_id: str, first_metric: str, second_metric: str,
                       start: str, end: str) -> dict[str, Any]:
        require_identifier(campaign_id, "campaign_id")
        require_metric(first_metric)
        require_metric(second_metric)
        if first_metric == second_metric:
            raise DomainError("invalid_pair", "paired metrics must differ")
        with self.store.connect() as db:
            spec = self.store.get_spec(campaign_id, db=db)
            known = {rule.name for rule in spec.rules}
            if first_metric not in known or second_metric not in known:
                raise DomainError("unknown_metric", "paired metric is not declared")
            left, right = validate_window(spec, start, end)
            samples = self.store.list_samples(campaign_id, status="active", start=left, end=right, db=db)
            pairs: dict[str, list[tuple[Decimal, Decimal]]] = defaultdict(list)
            for sample in samples:
                measurements = sample["measurements"]
                if first_metric in measurements and second_metric in measurements:
                    pairs[sample["group_id"]].append((Decimal(measurements[first_metric]),
                                                      Decimal(measurements[second_metric])))
            result = {}
            for group in spec.groups:
                values = pairs[group]
                if not values:
                    result[group] = {"count": 0, "covariance": None, "correlation": None}
                    continue
                with localcontext() as context:
                    context.prec = 32
                    count = Decimal(len(values))
                    x_mean = sum(x for x, _ in values) / count
                    y_mean = sum(y for _, y in values) / count
                    covariance = sum((x - x_mean) * (y - y_mean) for x, y in values) / count
                    x_variance = sum((x - x_mean) ** 2 for x, _ in values) / count
                    y_variance = sum((y - y_mean) ** 2 for _, y in values) / count
                    divisor = (x_variance * y_variance).sqrt() if x_variance and y_variance else None
                    correlation = str(covariance / divisor) if divisor else None
                    result[group] = {"count": len(values), "covariance": str(covariance),
                                     "correlation": correlation}
            return {"campaign_id": campaign_id, "first_metric": first_metric,
                    "second_metric": second_metric, "window": {"start": left, "end": right},
                    "groups": result}
