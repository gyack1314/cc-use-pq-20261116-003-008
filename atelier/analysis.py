from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, localcontext
import json
from typing import Any

from .domain import DomainError, canonical, digest, require_identifier, utc_now
from .quality import metric_health, summarize_quality, validate_window
from .store import Store


def statistics(values: list[Decimal]) -> dict[str, str | int] | None:
    if not values:
        return None
    with localcontext() as context:
        context.prec = 32
        count = len(values)
        mean = sum(values) / Decimal(count)
        variance = sum((value - mean) ** 2 for value in values) / Decimal(count)
        ordered = sorted(values)
        if count % 2:
            median = ordered[count // 2]
        else:
            median = (ordered[count // 2 - 1] + ordered[count // 2]) / 2
        return {"count": count, "mean": str(mean), "median": str(median),
                "minimum": str(ordered[0]), "maximum": str(ordered[-1]),
                "variance": str(variance)}


def group_statistics(spec, samples: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[tuple[str, str], list[Decimal]] = defaultdict(list)
    for sample in samples:
        if sample["status"] != "active":
            continue
        for name, value in sample["measurements"].items():
            grouped[(sample["group_id"], name)].append(Decimal(value))
    result = {}
    for group in spec.groups:
        result[group] = {rule.name: statistics(grouped[(group, rule.name)]) for rule in spec.rules}
    return result


def contrasts(spec, groups: dict[str, Any]) -> dict[str, Any]:
    baseline = groups[spec.baseline]
    result = {}
    for group in spec.groups:
        if group == spec.baseline:
            continue
        metrics = {}
        for rule in spec.rules:
            reference = baseline[rule.name]
            candidate = groups[group][rule.name]
            if reference is None or candidate is None:
                metrics[rule.name] = {"difference": None, "relative_percent": None}
                continue
            mean_a = Decimal(reference["mean"])
            mean_b = Decimal(candidate["mean"])
            difference = mean_b - mean_a
            relative = None if mean_a == 0 else str(difference / abs(mean_a) * Decimal(100))
            metrics[rule.name] = {"difference": str(difference), "relative_percent": relative}
        result[group] = metrics
    return result


class Analyzer:
    def __init__(self, store: Store):
        self.store = store

    def build(self, campaign_id: str, analysis_id: str, start: str, end: str,
              actor: str) -> dict[str, Any]:
        require_identifier(campaign_id, "campaign_id")
        require_identifier(analysis_id, "analysis_id")
        require_identifier(actor, "actor")
        with self.store.transaction() as db:
            spec = self.store.get_spec(campaign_id, db=db)
            start, end = validate_window(spec, start, end)
            request = {"campaign_id": campaign_id, "start": start, "end": end,
                       "spec_version": spec.version}
            request_sha = digest(request)
            current = self.store.get_campaign(campaign_id, db)
            old = db.execute("SELECT request_sha,source_revision,body FROM analyses"
                             " WHERE campaign_id=? AND analysis_id=?", (campaign_id, analysis_id)).fetchone()
            if old:
                if old["request_sha"] != request_sha:
                    raise DomainError("analysis_conflict", "analysis id names a different request")
                return {**json.loads(old["body"]), "replayed": True,
                        "current": old["source_revision"] == current["revision"]}
            samples = self.store.list_samples(campaign_id, start=start, end=end, db=db)
            rejection_rows = db.execute("SELECT batch_id,reason,detail,event_time FROM rejections"
                                        " WHERE campaign_id=? AND event_time>=? AND event_time<?"
                                        " ORDER BY batch_id,ordinal", (campaign_id, start, end)).fetchall()
            rejections = [dict(row) for row in rejection_rows]
            quality = summarize_quality(spec, samples, rejections, start, end)
            groups = group_statistics(spec, samples)
            result = {"campaign_id": campaign_id, "analysis_id": analysis_id,
                      "request": request, "source_revision": current["revision"],
                      "quality": quality, "metric_health": metric_health(spec, samples),
                      "groups": groups, "contrasts": contrasts(spec, groups),
                      "created_at": utc_now(), "replayed": False, "current": True}
            result_sha = digest(result)
            result["result_sha"] = result_sha
            db.execute("INSERT INTO analyses(campaign_id,analysis_id,request_sha,source_revision,body,"
                       "result_sha,created_at) VALUES(?,?,?,?,?,?,?)",
                       (campaign_id, analysis_id, request_sha, current["revision"], canonical(result),
                        result_sha, result["created_at"]))
            self.store.audit(db, campaign_id, "analysis_created", actor, analysis_id, None,
                             {"result_sha": result_sha, "source_revision": current["revision"]})
            return result

    def status(self, campaign_id: str, analysis_id: str) -> dict[str, Any]:
        with self.store.connect() as db:
            analysis = self.store.get_analysis(campaign_id, analysis_id, db)
            campaign = self.store.get_campaign(campaign_id, db)
            return {"analysis_id": analysis_id, "source_revision": analysis["source_revision"],
                    "current_revision": campaign["revision"],
                    "current": analysis["source_revision"] == campaign["revision"],
                    "result_sha": analysis["result_sha"]}

    def list_analyses(self, campaign_id: str, limit: int = 100) -> list[dict[str, Any]]:
        with self.store.connect() as db:
            self.store.get_campaign(campaign_id, db)
            rows = db.execute("SELECT analysis_id,source_revision,result_sha,created_at"
                              " FROM analyses WHERE campaign_id=? ORDER BY created_at DESC LIMIT ?",
                              (campaign_id, limit)).fetchall()
            return [dict(row) for row in rows]
