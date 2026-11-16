from __future__ import annotations

from collections import Counter, defaultdict
from decimal import Decimal
import json
from typing import Any

from .domain import DomainError, digest, require_identifier
from .store import Store


class Reconciler:
    def __init__(self, store: Store):
        self.store = store

    def source_revision(self, campaign_id: str) -> dict[str, Any]:
        require_identifier(campaign_id, "campaign_id")
        with self.store.connect() as db:
            campaign = self.store.get_campaign(campaign_id, db)
            events = db.execute("SELECT kind,subject,id FROM audits WHERE campaign_id=? ORDER BY id",
                                (campaign_id,)).fetchall()
            mutating = {"spec_updated", "sample_corrected", "sample_voided"}
            revision_events = [dict(row) for row in events if row["kind"] in mutating]
            batch_rows = db.execute("SELECT batch_id,accepted FROM batches WHERE campaign_id=?",
                                    (campaign_id,)).fetchall()
            revision_events.extend({"kind": "batch_ingested", "subject": row["batch_id"]}
                                   for row in batch_rows if row["accepted"] > 0)
            expected = len(revision_events)
            return {"campaign_id": campaign_id, "recorded": campaign["revision"],
                    "derived": expected, "matches": expected == campaign["revision"],
                    "events": revision_events}

    def sample_counts(self, campaign_id: str) -> dict[str, Any]:
        require_identifier(campaign_id, "campaign_id")
        with self.store.connect() as db:
            self.store.get_campaign(campaign_id, db)
            batch_rows = db.execute("SELECT batch_id,accepted,rejected FROM batches WHERE campaign_id=?",
                                    (campaign_id,)).fetchall()
            records = []
            for batch in batch_rows:
                accepted = db.execute("SELECT count(*) FROM samples WHERE campaign_id=? AND batch_id=?",
                                      (campaign_id, batch["batch_id"])).fetchone()[0]
                rejected = db.execute("SELECT count(*) FROM rejections WHERE campaign_id=? AND batch_id=?",
                                      (campaign_id, batch["batch_id"])).fetchone()[0]
                records.append({"batch_id": batch["batch_id"],
                                "recorded_accepted": batch["accepted"], "actual_accepted": accepted,
                                "recorded_rejected": batch["rejected"], "actual_rejected": rejected,
                                "matches": batch["accepted"] == accepted and batch["rejected"] == rejected})
            return {"campaign_id": campaign_id, "matches": all(row["matches"] for row in records),
                    "batches": records}

    def analysis_freshness(self, campaign_id: str) -> dict[str, Any]:
        require_identifier(campaign_id, "campaign_id")
        with self.store.connect() as db:
            campaign = self.store.get_campaign(campaign_id, db)
            rows = db.execute("SELECT analysis_id,source_revision,result_sha,body FROM analyses"
                              " WHERE campaign_id=? ORDER BY analysis_id", (campaign_id,)).fetchall()
            analyses = []
            for row in rows:
                body = json.loads(row["body"])
                body_sha = body.pop("result_sha", None)
                analyses.append({"analysis_id": row["analysis_id"],
                                 "source_revision": row["source_revision"],
                                 "current_revision": campaign["revision"],
                                 "fresh": row["source_revision"] == campaign["revision"],
                                 "digest_valid": body_sha == row["result_sha"] == digest(body)})
            return {"campaign_id": campaign_id, "analyses": analyses,
                    "invalid_digests": [row["analysis_id"] for row in analyses if not row["digest_valid"]]}

    def releases(self, campaign_id: str) -> dict[str, Any]:
        require_identifier(campaign_id, "campaign_id")
        with self.store.connect() as db:
            self.store.get_campaign(campaign_id, db)
            rows = db.execute("SELECT release_id,analysis_id,result_sha,body FROM releases"
                              " WHERE campaign_id=? ORDER BY release_id", (campaign_id,)).fetchall()
            releases = []
            for row in rows:
                body = json.loads(row["body"])
                release_sha = body.pop("release_sha", None)
                analysis = db.execute("SELECT result_sha FROM analyses WHERE campaign_id=? AND analysis_id=?",
                                      (campaign_id, row["analysis_id"])).fetchone()
                approvals = db.execute("SELECT reviewer,decision,result_sha FROM reviews"
                                       " WHERE campaign_id=? AND analysis_id=?", (campaign_id, row["analysis_id"])).fetchall()
                valid_approvals = {item["reviewer"] for item in approvals
                                   if item["decision"] == "approve" and item["result_sha"] == row["result_sha"]}
                rejected = any(item["decision"] == "reject" for item in approvals)
                result = {"release_id": row["release_id"],
                          "release_digest_valid": release_sha == digest(body),
                          "analysis_digest_valid": analysis is not None and analysis["result_sha"] == row["result_sha"],
                          "approvals": sorted(valid_approvals), "rejected": rejected}
                result["matches"] = (result["release_digest_valid"] and result["analysis_digest_valid"]
                                     and len(valid_approvals) >= 2 and not rejected)
                releases.append(result)
            return {"campaign_id": campaign_id, "matches": all(row["matches"] for row in releases),
                    "releases": releases}

    def full_report(self, campaign_id: str) -> dict[str, Any]:
        revision = self.source_revision(campaign_id)
        counts = self.sample_counts(campaign_id)
        analyses = self.analysis_freshness(campaign_id)
        releases = self.releases(campaign_id)
        return {"campaign_id": campaign_id,
                "ok": revision["matches"] and counts["matches"] and not analyses["invalid_digests"]
                      and releases["matches"],
                "revision": revision, "sample_counts": counts,
                "analysis_freshness": analyses, "releases": releases}
