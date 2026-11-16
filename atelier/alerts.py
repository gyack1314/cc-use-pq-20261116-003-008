from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
import json
import sqlite3
from typing import Any

from .analysis import group_statistics
from .domain import (CampaignSpec, DomainError, canonical, decimal, digest, integer,
                     parse_time, require_identifier, require_metric, utc_now)
from .quality import summarize_quality, window_bounds
from .store import Store

PERSISTENT = "persistent_deviation"
INCIDENTAL = "incidental_fluctuation"
INSUFFICIENT = "insufficient_evidence"
NORMAL = "normal"
CLASSIFICATIONS = {PERSISTENT, INCIDENTAL, INSUFFICIENT, NORMAL}
ACTIONABLE = {PERSISTENT, INCIDENTAL}
DISPOSITION_DECISIONS = {"confirm", "reject"}
BLOCKING_STATES = {"open", "stale", "suspended"}


def validate_monitor_payload(spec: CampaignSpec, payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise DomainError("invalid_monitor", "monitor must be an object")
    group_id = require_identifier(payload.get("group_id"), "group_id")
    if group_id not in spec.groups:
        raise DomainError("unknown_group", "monitor group is not declared")
    if group_id == spec.baseline:
        raise DomainError("invalid_monitor", "monitor group cannot be the baseline group")
    metric = require_metric(payload.get("metric"))
    if metric not in {rule.name for rule in spec.rules}:
        raise DomainError("unknown_metric", "monitor metric is not declared")
    threshold = decimal(payload.get("deviation_percent"), "deviation_percent")
    if threshold <= 0:
        raise DomainError("invalid_monitor", "deviation_percent must be positive")
    window_count = integer(payload.get("window_count", 3), "window_count", 1, 200)
    persistence = integer(payload.get("persistence", 2), "persistence", 1, 200)
    if persistence > window_count:
        raise DomainError("invalid_monitor", "persistence cannot exceed window_count")
    anchor = parse_time(payload.get("anchor"))
    return {"group_id": group_id, "metric": metric,
            "deviation_percent": str(threshold), "window_count": window_count,
            "persistence": persistence, "anchor": anchor}


def build_sequence(spec: CampaignSpec, anchor: str, window_count: int) -> tuple[str, list[dict[str, str]], str]:
    start, _ = window_bounds(anchor, spec.window_hours)
    cursor = datetime.fromisoformat(start)
    windows: list[dict[str, str]] = []
    for index in range(window_count):
        left = cursor
        right = left + timedelta(hours=spec.window_hours)
        windows.append({"index": index,
                        "start": left.isoformat(timespec="microseconds"),
                        "end": right.isoformat(timespec="microseconds")})
        cursor = right
    sequence_sha = digest([window["start"] for window in windows])
    return start, windows, sequence_sha


def derive_state(judgment_body: dict[str, Any],
                 latest_disposition: dict[str, Any] | None) -> tuple[str, bool | None, bool]:
    """Return (state, still_applies, publishable). State is derived, never stored."""
    qualified = bool(judgment_body["all_qualified"])
    if not qualified:
        return "suspended", (False if latest_disposition else None), False
    if latest_disposition is None:
        return "open", None, False
    still_applies = latest_disposition["judgment_sha"] == judgment_body["judgment_sha"]
    if not still_applies:
        return "stale", False, False
    if latest_disposition["decision"] == "confirm":
        return "confirmed", True, True
    return "rejected", True, True


class AlertManager:
    def __init__(self, store: Store):
        self.store = store

    def create_monitor(self, campaign_id: str, monitor_id: str,
                       payload: dict[str, Any], actor: str) -> dict[str, Any]:
        require_identifier(campaign_id, "campaign_id")
        require_identifier(monitor_id, "monitor_id")
        require_identifier(actor, "actor")
        with self.store.transaction() as db:
            if db.execute("SELECT 1 FROM monitors WHERE campaign_id=? AND monitor_id=?",
                          (campaign_id, monitor_id)).fetchone():
                raise DomainError("conflict", "monitor already exists")
            spec = self.store.get_spec(campaign_id, db=db)
            settings = validate_monitor_payload(spec, payload)
            start, windows, sequence_sha = build_sequence(spec, settings["anchor"], settings["window_count"])
            body = {"campaign_id": campaign_id, "monitor_id": monitor_id,
                    "spec_version": spec.version, "window_hours": spec.window_hours,
                    "baseline": spec.baseline, **settings,
                    "sequence_start": start,
                    "sequence_end": windows[-1]["end"], "sequence_sha": sequence_sha,
                    "created_at": utc_now()}
            db.execute("INSERT INTO monitors(campaign_id,monitor_id,body,content_sha,created_at)"
                       " VALUES(?,?,?,?,?)",
                       (campaign_id, monitor_id, canonical(body), digest(body), body["created_at"]))
            self.store.audit(db, campaign_id, "monitor_created", actor, monitor_id, None, body)
            return body

    def get_monitor(self, campaign_id: str, monitor_id: str,
                    db: sqlite3.Connection | None = None) -> dict[str, Any]:
        if db is None:
            with self.store.connect() as local:
                return self.get_monitor(campaign_id, monitor_id, local)
        row = self.store.one(db, "SELECT body FROM monitors WHERE campaign_id=? AND monitor_id=?",
                             (campaign_id, monitor_id))
        return json.loads(row["body"])

    def list_monitors(self, campaign_id: str) -> list[dict[str, Any]]:
        require_identifier(campaign_id, "campaign_id")
        with self.store.connect() as db:
            self.store.get_campaign(campaign_id, db)
            rows = db.execute("SELECT body FROM monitors WHERE campaign_id=? ORDER BY monitor_id",
                              (campaign_id,)).fetchall()
            return [json.loads(row["body"]) for row in rows]

    def _window_evidence(self, db, spec: CampaignSpec, campaign_id: str,
                         window: dict[str, str], monitor: dict[str, Any]) -> dict[str, Any]:
        samples = self.store.list_samples(campaign_id, start=window["start"], end=window["end"], db=db)
        rejection_rows = db.execute(
            "SELECT batch_id,reason,detail,event_time FROM rejections"
            " WHERE campaign_id=? AND event_time>=? AND event_time<? ORDER BY batch_id,ordinal",
            (campaign_id, window["start"], window["end"])).fetchall()
        quality = summarize_quality(spec, samples, [dict(row) for row in rejection_rows],
                                    window["start"], window["end"])
        groups = group_statistics(spec, samples)
        baseline_stats = groups[spec.baseline][monitor["metric"]]
        target_stats = groups[monitor["group_id"]][monitor["metric"]]
        deviation_percent = None
        if baseline_stats is not None and target_stats is not None:
            baseline_mean = Decimal(baseline_stats["mean"])
            target_mean = Decimal(target_stats["mean"])
            if baseline_mean != 0:
                deviation_percent = str((target_mean - baseline_mean) / abs(baseline_mean) * Decimal(100))
        deviated = (deviation_percent is not None
                    and abs(Decimal(deviation_percent)) >= Decimal(monitor["deviation_percent"]))
        return {"index": window["index"], "window": {"start": window["start"], "end": window["end"]},
                "qualified": quality["passed"], "gates": quality["gates"],
                "group_counts": quality["group_counts"], "rejected_count": quality["rejected_count"],
                "baseline_mean": baseline_stats["mean"] if baseline_stats else None,
                "target_mean": target_stats["mean"] if target_stats else None,
                "deviation_percent": deviation_percent,
                "threshold_percent": monitor["deviation_percent"], "deviated": deviated}

    @staticmethod
    def classify(monitor: dict[str, Any], evidence: list[dict[str, Any]]) -> tuple[str, int]:
        qualified = [item for item in evidence if item["qualified"]]
        longest_run = run = 0
        for item in evidence:
            if item["qualified"] and item["deviated"]:
                run += 1
                longest_run = max(longest_run, run)
            else:
                run = 0
        if len(qualified) < monitor["persistence"]:
            return INSUFFICIENT, longest_run
        if longest_run >= monitor["persistence"]:
            return PERSISTENT, longest_run
        if any(item["qualified"] and item["deviated"] for item in evidence):
            return INCIDENTAL, longest_run
        return NORMAL, longest_run

    def evaluate(self, campaign_id: str, monitor_id: str, actor: str) -> dict[str, Any]:
        require_identifier(campaign_id, "campaign_id")
        require_identifier(monitor_id, "monitor_id")
        require_identifier(actor, "actor")
        with self.store.transaction() as db:
            monitor = self.get_monitor(campaign_id, monitor_id, db)
            spec = self.store.get_spec(campaign_id, monitor["spec_version"], db)
            campaign = self.store.get_campaign(campaign_id, db)
            alert_id = "alert-" + monitor_id
            _, windows, sequence_sha = build_sequence(spec, monitor["anchor"], monitor["window_count"])
            request = {"campaign_id": campaign_id, "monitor_id": monitor_id,
                       "spec_version": spec.version, "window_hours": spec.window_hours,
                       "window_count": monitor["window_count"], "anchor": monitor["anchor"],
                       "group_id": monitor["group_id"], "metric": monitor["metric"],
                       "deviation_percent": monitor["deviation_percent"],
                       "persistence": monitor["persistence"],
                       "sequence_start": windows[0]["start"], "sequence_end": windows[-1]["end"]}
            request_sha = digest(request)
            alert_row = db.execute("SELECT * FROM alerts WHERE campaign_id=? AND alert_id=?",
                                   (campaign_id, alert_id)).fetchone()
            if alert_row:
                latest = db.execute("SELECT * FROM alert_judgments WHERE campaign_id=? AND alert_id=?"
                                    " ORDER BY judgment_version DESC LIMIT 1",
                                    (campaign_id, alert_id)).fetchone()
                if latest["request_sha"] == request_sha and latest["source_revision"] == campaign["revision"]:
                    return {**self._detail(db, campaign_id, alert_id), "replayed": True}
                next_version = alert_row["current_version"] + 1
            else:
                latest = None
                next_version = 1
            evidence = [self._window_evidence(db, spec, campaign_id, window, monitor) for window in windows]
            classification, longest_run = self.classify(monitor, evidence)
            qualified_count = sum(1 for item in evidence if item["qualified"])
            all_qualified = qualified_count == len(evidence)
            body = {"campaign_id": campaign_id, "alert_id": alert_id, "monitor_id": monitor_id,
                    "judgment_version": next_version, "classification": classification,
                    "request": request, "request_sha": request_sha,
                    "source_revision": campaign["revision"], "spec_version": spec.version,
                    "sequence_sha": sequence_sha, "windows": evidence,
                    "qualified_count": qualified_count,
                    "deviated_qualified_count": sum(1 for item in evidence
                                                    if item["qualified"] and item["deviated"]),
                    "longest_deviation_run": longest_run, "all_qualified": all_qualified,
                    "created_at": utc_now()}
            judgment_sha = digest(body)
            body["judgment_sha"] = judgment_sha
            now = utc_now()
            if alert_row is None:
                db.execute("INSERT INTO alerts(campaign_id,alert_id,monitor_id,sequence_sha,"
                           "current_version,created_at) VALUES(?,?,?,?,?,?)",
                           (campaign_id, alert_id, monitor_id, sequence_sha, next_version, now))
            else:
                db.execute("UPDATE alerts SET current_version=?,sequence_sha=? WHERE campaign_id=? AND alert_id=?",
                           (next_version, sequence_sha, campaign_id, alert_id))
            db.execute(
                "INSERT INTO alert_judgments(campaign_id,alert_id,judgment_version,source_revision,"
                "spec_version,request_sha,judgment_sha,classification,all_qualified,body,created_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (campaign_id, alert_id, next_version, campaign["revision"], spec.version,
                 request_sha, judgment_sha, classification, int(all_qualified),
                 canonical(body), body["created_at"]))
            self.store.audit(db, campaign_id, "alert_judged", actor, alert_id,
                             None if latest is None else {"judgment_version": latest["judgment_version"]},
                             {"judgment_version": next_version, "classification": classification,
                              "source_revision": campaign["revision"], "judgment_sha": judgment_sha,
                              "all_qualified": all_qualified})
            return {**self._detail(db, campaign_id, alert_id), "replayed": False}

    def _dispositions(self, db, campaign_id: str, alert_id: str) -> list[dict[str, Any]]:
        rows = db.execute("SELECT reviewer,decision,note,judgment_sha,judgment_version,created_at"
                          " FROM alert_dispositions WHERE campaign_id=? AND alert_id=? ORDER BY id",
                          (campaign_id, alert_id)).fetchall()
        return [dict(row) for row in rows]

    def _detail(self, db, campaign_id: str, alert_id: str) -> dict[str, Any]:
        alert_row = self.store.one(db, "SELECT * FROM alerts WHERE campaign_id=? AND alert_id=?",
                                   (campaign_id, alert_id))
        latest = self.store.one(db, "SELECT * FROM alert_judgments WHERE campaign_id=? AND alert_id=?"
                                    " ORDER BY judgment_version DESC LIMIT 1", (campaign_id, alert_id))
        judgment_body = json.loads(latest["body"])
        dispositions = self._dispositions(db, campaign_id, alert_id)
        latest_disposition = dispositions[-1] if dispositions else None
        state, still_applies, publishable = derive_state(judgment_body, latest_disposition)
        campaign = self.store.get_campaign(campaign_id, db)
        return {"campaign_id": campaign_id, "alert_id": alert_id,
                "monitor_id": alert_row["monitor_id"], "sequence_sha": alert_row["sequence_sha"],
                "state": state, "classification": judgment_body["classification"],
                "current_version": alert_row["current_version"],
                "judgment_version": latest["judgment_version"],
                "judgment_sha": latest["judgment_sha"],
                "source_revision": latest["source_revision"],
                "current_revision": campaign["revision"],
                "all_qualified": bool(latest["all_qualified"]),
                "still_applies": still_applies, "publishable": publishable,
                "latest_disposition": latest_disposition, "dispositions": dispositions,
                "latest_judgment": judgment_body, "created_at": alert_row["created_at"]}

    def get_alert(self, campaign_id: str, alert_id: str) -> dict[str, Any]:
        require_identifier(campaign_id, "campaign_id")
        require_identifier(alert_id, "alert_id")
        with self.store.connect() as db:
            return self._detail(db, campaign_id, alert_id)

    def list_alerts(self, campaign_id: str, state: str | None = None) -> dict[str, Any]:
        require_identifier(campaign_id, "campaign_id")
        if state is not None and state not in {"open", "confirmed", "rejected", "stale", "suspended"}:
            raise DomainError("invalid_state", "unknown alert state filter")
        with self.store.connect() as db:
            self.store.get_campaign(campaign_id, db)
            rows = db.execute("SELECT alert_id FROM alerts WHERE campaign_id=? ORDER BY alert_id",
                              (campaign_id,)).fetchall()
            items = []
            for row in rows:
                detail = self._detail(db, campaign_id, row["alert_id"])
                if state is not None and detail["state"] != state:
                    continue
                items.append({key: detail[key] for key in (
                    "alert_id", "monitor_id", "state", "classification", "current_version",
                    "judgment_sha", "source_revision", "current_revision", "all_qualified",
                    "still_applies", "publishable", "latest_disposition", "sequence_sha")})
            return {"campaign_id": campaign_id, "items": items, "count": len(items)}

    def dispose(self, campaign_id: str, alert_id: str, reviewer: str,
                decision: str, note: Any, expected_sha: Any) -> dict[str, Any]:
        require_identifier(campaign_id, "campaign_id")
        require_identifier(alert_id, "alert_id")
        require_identifier(reviewer, "reviewer")
        if decision not in DISPOSITION_DECISIONS:
            raise DomainError("invalid_decision", "decision must be confirm or reject")
        if not isinstance(note, str) or not 8 <= len(note.strip()) <= 500:
            raise DomainError("invalid_note", "disposition note must have 8-500 characters")
        if not isinstance(expected_sha, str) or len(expected_sha) != 64 or \
                any(ch not in "0123456789abcdef" for ch in expected_sha):
            raise DomainError("invalid_sha", "expected_judgment_sha must be a SHA-256 digest")
        with self.store.transaction() as db:
            latest = db.execute("SELECT j.* FROM alert_judgments j JOIN alerts a"
                                " ON a.campaign_id=j.campaign_id AND a.alert_id=j.alert_id"
                                " WHERE j.campaign_id=? AND j.alert_id=?"
                                " ORDER BY j.judgment_version DESC LIMIT 1",
                                (campaign_id, alert_id)).fetchone()
            if latest is None:
                raise DomainError("not_found", "requested resource does not exist")
            judgment_body = json.loads(latest["body"])
            if latest["judgment_sha"] != expected_sha:
                raise DomainError("version_conflict",
                                  "alert judgment changed; re-evaluate before disposing")
            if judgment_body["classification"] == INSUFFICIENT:
                raise DomainError("alert_not_actionable",
                                  "insufficient evidence cannot be confirmed or rejected")
            if not judgment_body["all_qualified"]:
                raise DomainError("alert_suspended",
                                  "a trigger window is unqualified; alert cannot be dispositioned")
            existing = db.execute("SELECT * FROM alert_dispositions WHERE campaign_id=? AND alert_id=?"
                                  " AND reviewer=? AND judgment_sha=?",
                                  (campaign_id, alert_id, reviewer, expected_sha)).fetchone()
            record = {"campaign_id": campaign_id, "alert_id": alert_id, "reviewer": reviewer,
                      "decision": decision, "note": note.strip(),
                      "judgment_sha": expected_sha,
                      "judgment_version": latest["judgment_version"]}
            if existing:
                if existing["decision"] == decision and existing["note"] == note.strip():
                    return {**self._detail(db, campaign_id, alert_id), "replayed": True}
                raise DomainError("disposition_conflict",
                                  "reviewer already made a different decision on this judgment")
            now = utc_now()
            db.execute("INSERT INTO alert_dispositions(campaign_id,alert_id,reviewer,decision,note,"
                       "judgment_sha,judgment_version,created_at) VALUES(?,?,?,?,?,?,?,?)",
                       (campaign_id, alert_id, reviewer, decision, note.strip(),
                        expected_sha, latest["judgment_version"], now))
            self.store.audit(db, campaign_id, "alert_dispositioned", reviewer, alert_id, None,
                             {**record, "created_at": now})
            return {**self._detail(db, campaign_id, alert_id), "replayed": False}

    def list_dispositions(self, campaign_id: str, alert_id: str) -> list[dict[str, Any]]:
        require_identifier(campaign_id, "campaign_id")
        require_identifier(alert_id, "alert_id")
        with self.store.connect() as db:
            self.store.one(db, "SELECT 1 FROM alerts WHERE campaign_id=? AND alert_id=?",
                           (campaign_id, alert_id))
            return self._dispositions(db, campaign_id, alert_id)

    def list_judgments(self, campaign_id: str, alert_id: str) -> list[dict[str, Any]]:
        require_identifier(campaign_id, "campaign_id")
        require_identifier(alert_id, "alert_id")
        with self.store.connect() as db:
            self.store.one(db, "SELECT 1 FROM alerts WHERE campaign_id=? AND alert_id=?",
                           (campaign_id, alert_id))
            rows = db.execute("SELECT judgment_version,source_revision,spec_version,request_sha,"
                              "judgment_sha,classification,all_qualified,created_at"
                              " FROM alert_judgments WHERE campaign_id=? AND alert_id=?"
                              " ORDER BY judgment_version", (campaign_id, alert_id)).fetchall()
            return [{**dict(row), "all_qualified": bool(row["all_qualified"])} for row in rows]

    def evidence_for_window(self, db, campaign_id: str,
                            start: str, end: str) -> list[dict[str, Any]]:
        """Alert evidence whose trigger sequence overlaps [start, end)."""
        rows = db.execute("SELECT a.alert_id,a.monitor_id,j.body AS judgment_body,j.judgment_sha"
                          " FROM alerts a JOIN alert_judgments j ON a.campaign_id=j.campaign_id"
                          " AND a.alert_id=j.alert_id AND j.judgment_version=a.current_version"
                          " WHERE a.campaign_id=?", (campaign_id,)).fetchall()
        evidence = []
        for row in rows:
            judgment_body = json.loads(row["judgment_body"])
            overlaps = any(item["window"]["start"] < end and item["window"]["end"] > start
                           for item in judgment_body["windows"])
            if not overlaps:
                continue
            dispositions = self._dispositions(db, campaign_id, row["alert_id"])
            latest_disposition = dispositions[-1] if dispositions else None
            state, still_applies, publishable = derive_state(judgment_body, latest_disposition)
            evidence.append({"alert_id": row["alert_id"], "monitor_id": row["monitor_id"],
                             "state": state, "classification": judgment_body["classification"],
                             "judgment_version": judgment_body["judgment_version"],
                             "judgment_sha": row["judgment_sha"],
                             "source_revision": judgment_body["source_revision"],
                             "all_qualified": judgment_body["all_qualified"],
                             "still_applies": still_applies, "publishable": publishable,
                             "latest_disposition": latest_disposition,
                             "trigger_windows": [{"index": item["index"], "window": item["window"],
                                                  "qualified": item["qualified"], "gates": item["gates"],
                                                  "deviated": item["deviated"],
                                                  "deviation_percent": item["deviation_percent"]}
                                                 for item in judgment_body["windows"]]})
        return evidence

    def blockers_for_window(self, db, campaign_id: str,
                            start: str, end: str) -> list[dict[str, Any]]:
        blockers = []
        for item in self.evidence_for_window(db, campaign_id, start, end):
            reason = None
            if item["state"] == "suspended":
                reason = "suspended_alert_window"
            elif item["state"] == "stale":
                reason = "stale_alert_disposition"
            elif item["state"] == "open" and item["classification"] == PERSISTENT:
                reason = "open_persistent_alert"
            if reason:
                blockers.append({**item, "block_reason": reason})
        return blockers
