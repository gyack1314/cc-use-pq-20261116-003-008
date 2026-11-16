from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from .analysis import Analyzer
from .alerts import AlertManager
from .domain import CampaignSpec, DomainError, integer, parse_time, require_identifier
from .ingest import Ingestor
from .inspection import Inspector
from .evidence import EvidenceService
from .release import Publisher
from .reconcile import Reconciler
from .query import QueryService
from .explore import Explorer
from .lifecycle import Lifecycle
from .store import Store


def create_app(database_path: str | None = None) -> FastAPI:
    store = Store(database_path or os.environ.get("ATELIER_DB", "atelier.sqlite3"))
    ingestor = Ingestor(store)
    analyzer = Analyzer(store)
    publisher = Publisher(store)
    inspector = Inspector(store)
    evidence = EvidenceService(store)
    reconciler = Reconciler(store)
    query_service = QueryService(store)
    explorer = Explorer(store)
    lifecycle = Lifecycle(store)
    alert_manager = AlertManager(store)
    app = FastAPI(title="Atelier Experiment Ledger", version="1.0.0")

    @app.exception_handler(DomainError)
    async def domain_error_handler(_, error: DomainError):
        status = 404 if error.code == "not_found" else 409 if error.code.endswith("conflict") or error.code in {
            "stale_analysis", "inactive_sample", "release_conflict", "review_rejected",
            "reviews_missing", "self_publish", "quality_failed", "incompatible_spec",
            "alert_suspended", "alert_not_actionable", "alert_gate_blocked"} else 422
        return JSONResponse(status_code=status, content={"error": error.code, "detail": str(error)})

    @app.post("/campaigns", status_code=201)
    def create_campaign(payload: dict[str, Any]):
        actor = require_identifier(payload.get("actor"), "actor")
        spec = CampaignSpec.from_dict(payload)
        return store.create_campaign(spec, actor)

    @app.get("/campaigns/{campaign_id}")
    def get_campaign(campaign_id: str):
        return store.get_campaign(require_identifier(campaign_id, "campaign_id"))

    @app.get("/campaigns/{campaign_id}/overview")
    def overview(campaign_id: str):
        return inspector.campaign_overview(campaign_id)

    @app.get("/campaigns/{campaign_id}/coverage")
    def coverage(campaign_id: str, start: str, end: str):
        return inspector.coverage(campaign_id, start, end)

    @app.get("/campaigns/{campaign_id}/distribution/{metric}")
    def distribution(campaign_id: str, metric: str, start: str, end: str):
        return explorer.distribution(campaign_id, metric, start, end)

    @app.get("/campaigns/{campaign_id}/paired/{first_metric}/{second_metric}")
    def paired_metrics(campaign_id: str, first_metric: str, second_metric: str, start: str, end: str):
        return explorer.paired_metrics(campaign_id, first_metric, second_metric, start, end)

    @app.get("/campaigns/{campaign_id}/evidence/verify")
    def verify(campaign_id: str):
        return evidence.verify_campaign(campaign_id)

    @app.get("/campaigns/{campaign_id}/evidence/export")
    def export(campaign_id: str):
        return evidence.export(campaign_id)

    @app.get("/campaigns/{campaign_id}/reconciliation")
    def reconciliation(campaign_id: str):
        return reconciler.full_report(campaign_id)

    @app.get("/campaigns/{campaign_id}/lifecycle")
    def lifecycle_inventory(campaign_id: str):
        return lifecycle.inventory(campaign_id)

    @app.put("/campaigns/{campaign_id}/spec")
    def update_spec(campaign_id: str, payload: dict[str, Any]):
        actor = require_identifier(payload.get("actor"), "actor")
        expected = integer(payload.get("expected_version"), "expected_version", 1, 1000000)
        return store.update_spec(campaign_id, payload, expected, actor)

    @app.post("/campaigns/{campaign_id}/batches/{batch_id}")
    def ingest(campaign_id: str, batch_id: str, payload: dict[str, Any]):
        return ingestor.ingest(campaign_id, batch_id, payload.get("samples"), payload.get("actor"))

    @app.get("/campaigns/{campaign_id}/batches/{batch_id}")
    def get_batch(campaign_id: str, batch_id: str):
        return store.get_batch(campaign_id, batch_id)

    @app.get("/campaigns/{campaign_id}/batches/{batch_id}/report")
    def batch_report(campaign_id: str, batch_id: str):
        return inspector.batch_report(campaign_id, batch_id)

    @app.get("/campaigns/{campaign_id}/rejection-trends")
    def rejection_trends(campaign_id: str):
        return inspector.rejection_trends(campaign_id)

    @app.get("/campaigns/{campaign_id}/batches/{batch_id}/rejections")
    def rejections(campaign_id: str, batch_id: str):
        return ingestor.list_rejections(campaign_id, batch_id)

    @app.get("/campaigns/{campaign_id}/samples")
    def samples(campaign_id: str, status: str | None = None,
                start: str | None = None, end: str | None = None):
        if status is not None and status not in {"active", "void"}:
            raise DomainError("invalid_status", "status must be active or void")
        return store.list_samples(campaign_id, status=status,
                                  start=parse_time(start) if start else None,
                                  end=parse_time(end) if end else None)

    @app.get("/campaigns/{campaign_id}/sample-query")
    def sample_query(campaign_id: str, group_id: str | None = None,
                     status: str | None = None, metric: str | None = None,
                     minimum: str | None = None, maximum: str | None = None,
                     after: str | None = None, limit: int = 100):
        return query_service.samples(campaign_id, group_id=group_id, status=status,
                                     metric=metric, minimum=minimum, maximum=maximum,
                                     after=after, limit=limit)

    @app.get("/campaigns/{campaign_id}/batch-query")
    def batch_query(campaign_id: str, after: str | None = None, limit: int = 100):
        return query_service.batches(campaign_id, after=after, limit=limit)

    @app.put("/campaigns/{campaign_id}/samples/{sample_id}")
    def correct_sample(campaign_id: str, sample_id: str, payload: dict[str, Any]):
        return ingestor.correct(campaign_id, sample_id, payload.get("replacement"),
                                payload.get("expected_sha"), payload.get("actor"))

    @app.post("/campaigns/{campaign_id}/samples/{sample_id}/void")
    def void_sample(campaign_id: str, sample_id: str, payload: dict[str, Any]):
        return ingestor.void(campaign_id, sample_id, payload.get("expected_sha"),
                             payload.get("reason"), payload.get("actor"))

    @app.post("/campaigns/{campaign_id}/analyses/{analysis_id}")
    def build_analysis(campaign_id: str, analysis_id: str, payload: dict[str, Any]):
        return analyzer.build(campaign_id, analysis_id, payload.get("start"),
                              payload.get("end"), payload.get("actor"))

    @app.get("/campaigns/{campaign_id}/analyses")
    def list_analyses(campaign_id: str, limit: int = Query(100, ge=1, le=500)):
        return analyzer.list_analyses(campaign_id, limit)

    @app.get("/campaigns/{campaign_id}/analyses/{analysis_id}")
    def get_analysis(campaign_id: str, analysis_id: str):
        return store.get_analysis(campaign_id, analysis_id)["body"]

    @app.get("/campaigns/{campaign_id}/analyses/{analysis_id}/status")
    def analysis_status(campaign_id: str, analysis_id: str):
        return analyzer.status(campaign_id, analysis_id)

    @app.get("/campaigns/{campaign_id}/analyses/{first_id}/compare/{second_id}")
    def compare_analyses(campaign_id: str, first_id: str, second_id: str):
        return inspector.compare_analyses(campaign_id, first_id, second_id)

    @app.post("/campaigns/{campaign_id}/analyses/{analysis_id}/reviews")
    def review(campaign_id: str, analysis_id: str, payload: dict[str, Any]):
        return publisher.review(campaign_id, analysis_id, payload.get("reviewer"),
                                payload.get("decision"), payload.get("note"), payload.get("expected_sha"))

    @app.get("/campaigns/{campaign_id}/analyses/{analysis_id}/reviews")
    def list_reviews(campaign_id: str, analysis_id: str):
        return publisher.list_reviews(campaign_id, analysis_id)

    @app.post("/campaigns/{campaign_id}/releases/{release_id}")
    def publish(campaign_id: str, release_id: str, payload: dict[str, Any]):
        return publisher.publish(campaign_id, payload.get("analysis_id"), release_id,
                                 payload.get("actor"), payload.get("expected_sha"))

    @app.get("/campaigns/{campaign_id}/releases")
    def list_releases(campaign_id: str):
        return publisher.list_releases(campaign_id)

    @app.get("/campaigns/{campaign_id}/releases/{release_id}")
    def get_release(campaign_id: str, release_id: str):
        return store.get_release(campaign_id, release_id)["body"]

    @app.get("/campaigns/{campaign_id}/releases/{release_id}/context")
    def release_context(campaign_id: str, release_id: str):
        return inspector.release_context(campaign_id, release_id)

    @app.post("/campaigns/{campaign_id}/monitors/{monitor_id}", status_code=201)
    def create_monitor(campaign_id: str, monitor_id: str, payload: dict[str, Any]):
        return alert_manager.create_monitor(
            require_identifier(campaign_id, "campaign_id"),
            require_identifier(monitor_id, "monitor_id"), payload,
            require_identifier(payload.get("actor"), "actor"))

    @app.get("/campaigns/{campaign_id}/monitors")
    def list_monitors(campaign_id: str):
        return alert_manager.list_monitors(require_identifier(campaign_id, "campaign_id"))

    @app.get("/campaigns/{campaign_id}/monitors/{monitor_id}")
    def get_monitor(campaign_id: str, monitor_id: str):
        return alert_manager.get_monitor(
            require_identifier(campaign_id, "campaign_id"),
            require_identifier(monitor_id, "monitor_id"))

    @app.post("/campaigns/{campaign_id}/monitors/{monitor_id}/evaluate")
    def evaluate_monitor(campaign_id: str, monitor_id: str, payload: dict[str, Any]):
        return alert_manager.evaluate(
            require_identifier(campaign_id, "campaign_id"),
            require_identifier(monitor_id, "monitor_id"),
            require_identifier(payload.get("actor"), "actor"))

    @app.get("/campaigns/{campaign_id}/alerts")
    def list_alerts(campaign_id: str, state: str | None = None):
        return alert_manager.list_alerts(require_identifier(campaign_id, "campaign_id"), state)

    @app.get("/campaigns/{campaign_id}/alerts/{alert_id}")
    def get_alert(campaign_id: str, alert_id: str):
        return alert_manager.get_alert(
            require_identifier(campaign_id, "campaign_id"),
            require_identifier(alert_id, "alert_id"))

    @app.get("/campaigns/{campaign_id}/alerts/{alert_id}/judgments")
    def alert_judgments(campaign_id: str, alert_id: str):
        return alert_manager.list_judgments(
            require_identifier(campaign_id, "campaign_id"),
            require_identifier(alert_id, "alert_id"))

    @app.get("/campaigns/{campaign_id}/alerts/{alert_id}/dispositions")
    def alert_dispositions(campaign_id: str, alert_id: str):
        return alert_manager.list_dispositions(
            require_identifier(campaign_id, "campaign_id"),
            require_identifier(alert_id, "alert_id"))

    @app.post("/campaigns/{campaign_id}/alerts/{alert_id}/dispositions")
    def dispose_alert(campaign_id: str, alert_id: str, payload: dict[str, Any]):
        return alert_manager.dispose(
            require_identifier(campaign_id, "campaign_id"),
            require_identifier(alert_id, "alert_id"),
            require_identifier(payload.get("reviewer"), "reviewer"),
            payload.get("decision"), payload.get("note"),
            payload.get("expected_judgment_sha"))

    @app.get("/campaigns/{campaign_id}/analyses/{analysis_id}/alert-evidence")
    def analysis_alert_evidence(campaign_id: str, analysis_id: str):
        campaign_key = require_identifier(campaign_id, "campaign_id")
        analysis_key = require_identifier(analysis_id, "analysis_id")
        with store.connect() as db:
            analysis = store.get_analysis(campaign_key, analysis_key, db)
            window = analysis["body"]["request"]
            evidence = alert_manager.evidence_for_window(db, campaign_key,
                                                         window["start"], window["end"])
            blockers = alert_manager.blockers_for_window(db, campaign_key,
                                                         window["start"], window["end"])
        return {"campaign_id": campaign_key, "analysis_id": analysis_key,
                "window": {"start": window["start"], "end": window["end"]},
                "evidence": evidence, "blockers": blockers,
                "publishable": not blockers}

    @app.get("/campaigns/{campaign_id}/audits")
    def audits(campaign_id: str, since_id: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=500)):
        return store.list_audits(campaign_id, since_id, limit)

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app


app = create_app()
