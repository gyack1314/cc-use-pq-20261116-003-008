from __future__ import annotations

from collections import Counter
import json
from typing import Any

from .domain import DomainError, digest, require_identifier
from .store import Store


class EvidenceService:
    def __init__(self, store: Store):
        self.store = store

    def verify_campaign(self, campaign_id: str) -> dict[str, Any]:
        require_identifier(campaign_id, "campaign_id")
        with self.store.connect() as db:
            campaign = self.store.get_campaign(campaign_id, db)
            issues = []
            specs = db.execute("SELECT version,body,content_sha FROM specs WHERE campaign_id=? ORDER BY version",
                               (campaign_id,)).fetchall()
            expected_versions = list(range(1, campaign["spec"]["version"] + 1))
            actual_versions = [row["version"] for row in specs]
            if actual_versions != expected_versions:
                issues.append({"kind": "spec_gap", "expected": expected_versions, "actual": actual_versions})
            for row in specs:
                if digest(json.loads(row["body"])) != row["content_sha"]:
                    issues.append({"kind": "spec_digest", "version": row["version"]})
            sample_rows = db.execute("SELECT sample_id,group_id,event_time,spec_version,measurements,"
                                     "content_sha,status FROM samples WHERE campaign_id=? ORDER BY sample_id",
                                     (campaign_id,)).fetchall()
            for row in sample_rows:
                if row["status"] not in {"active", "void"}:
                    issues.append({"kind": "sample_status", "sample_id": row["sample_id"]})
                if row["spec_version"] not in actual_versions:
                    issues.append({"kind": "unknown_spec", "sample_id": row["sample_id"]})
                batch = db.execute("SELECT 1 FROM batches WHERE campaign_id=? AND batch_id="
                                   "(SELECT batch_id FROM samples WHERE campaign_id=? AND sample_id=?)",
                                   (campaign_id, campaign_id, row["sample_id"])).fetchone()
                if batch is None:
                    issues.append({"kind": "missing_batch", "sample_id": row["sample_id"]})
            batch_rows = db.execute("SELECT batch_id,accepted,rejected FROM batches WHERE campaign_id=?",
                                    (campaign_id,)).fetchall()
            for batch in batch_rows:
                current_accepted = db.execute("SELECT count(*) FROM samples WHERE campaign_id=? AND batch_id=?",
                                              (campaign_id, batch["batch_id"])).fetchone()[0]
                current_rejected = db.execute("SELECT count(*) FROM rejections WHERE campaign_id=? AND batch_id=?",
                                              (campaign_id, batch["batch_id"])).fetchone()[0]
                if current_accepted != batch["accepted"]:
                    issues.append({"kind": "batch_accepted_count", "batch_id": batch["batch_id"]})
                if current_rejected != batch["rejected"]:
                    issues.append({"kind": "batch_rejected_count", "batch_id": batch["batch_id"]})
            analysis_rows = db.execute("SELECT analysis_id,body,result_sha FROM analyses WHERE campaign_id=?",
                                       (campaign_id,)).fetchall()
            for row in analysis_rows:
                body = json.loads(row["body"])
                supplied = body.pop("result_sha", None)
                if digest(body) != row["result_sha"] or supplied != row["result_sha"]:
                    issues.append({"kind": "analysis_digest", "analysis_id": row["analysis_id"]})
            release_rows = db.execute("SELECT release_id,body,result_sha FROM releases WHERE campaign_id=?",
                                      (campaign_id,)).fetchall()
            for row in release_rows:
                body = json.loads(row["body"])
                supplied = body.pop("release_sha", None)
                if digest(body) != supplied or body["result_sha"] != row["result_sha"]:
                    issues.append({"kind": "release_digest", "release_id": row["release_id"]})
            audits = db.execute("SELECT id,kind,subject FROM audits WHERE campaign_id=? ORDER BY id",
                                (campaign_id,)).fetchall()
            event_kinds = Counter(row["kind"] for row in audits)
            return {"campaign_id": campaign_id, "ok": not issues, "issues": issues,
                    "counts": {"spec_versions": len(specs), "batches": len(batch_rows),
                               "samples": len(sample_rows), "analyses": len(analysis_rows),
                               "releases": len(release_rows), "audits": len(audits)},
                    "event_kinds": dict(sorted(event_kinds.items()))}

    def export(self, campaign_id: str) -> dict[str, Any]:
        require_identifier(campaign_id, "campaign_id")
        verification = self.verify_campaign(campaign_id)
        if not verification["ok"]:
            raise DomainError("integrity_failed", "campaign evidence failed verification")
        with self.store.connect() as db:
            campaign = self.store.get_campaign(campaign_id, db)
            specs = [json.loads(row[0]) for row in db.execute(
                "SELECT body FROM specs WHERE campaign_id=? ORDER BY version", (campaign_id,))]
            batches = [dict(row) for row in db.execute(
                "SELECT batch_id,payload_sha,spec_version,accepted,rejected,created_at"
                " FROM batches WHERE campaign_id=? ORDER BY created_at,batch_id", (campaign_id,))]
            samples = self.store.list_samples(campaign_id, db=db)
            rejections = [dict(row) for row in db.execute(
                "SELECT batch_id,ordinal,sample_id,reason,detail,event_time"
                " FROM rejections WHERE campaign_id=? ORDER BY batch_id,ordinal", (campaign_id,))]
            analyses = [dict(row) for row in db.execute(
                "SELECT analysis_id,request_sha,source_revision,result_sha,created_at"
                " FROM analyses WHERE campaign_id=? ORDER BY created_at,analysis_id", (campaign_id,))]
            reviews = [dict(row) for row in db.execute(
                "SELECT analysis_id,reviewer,decision,note,result_sha,created_at"
                " FROM reviews WHERE campaign_id=? ORDER BY analysis_id,reviewer", (campaign_id,))]
            releases = [dict(row) for row in db.execute(
                "SELECT release_id,analysis_id,source_revision,result_sha,created_at"
                " FROM releases WHERE campaign_id=? ORDER BY created_at,release_id", (campaign_id,))]
            audits = self.store.list_audits(campaign_id, limit=100000)
            payload = {"campaign": campaign, "specs": specs, "batches": batches,
                       "samples": samples, "rejections": rejections, "analyses": analyses,
                       "reviews": reviews, "releases": releases, "audits": audits}
            return {"campaign_id": campaign_id, "payload": payload, "digest": digest(payload),
                    "verification": verification}
