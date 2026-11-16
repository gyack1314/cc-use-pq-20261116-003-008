from __future__ import annotations

import json
from typing import Any

from .domain import DomainError, canonical, digest, require_identifier, utc_now
from .store import Store


class Publisher:
    def __init__(self, store: Store):
        self.store = store

    def review(self, campaign_id: str, analysis_id: str, reviewer: str,
               decision: str, note: str, expected_sha: str) -> dict[str, Any]:
        require_identifier(campaign_id, "campaign_id")
        require_identifier(analysis_id, "analysis_id")
        require_identifier(reviewer, "reviewer")
        if decision not in {"approve", "reject"}:
            raise DomainError("invalid_decision", "decision must be approve or reject")
        if not isinstance(note, str) or not 8 <= len(note.strip()) <= 500:
            raise DomainError("invalid_note", "review note must have 8-500 characters")
        with self.store.transaction() as db:
            analysis = self.store.get_analysis(campaign_id, analysis_id, db)
            campaign = self.store.get_campaign(campaign_id, db)
            if analysis["result_sha"] != expected_sha:
                raise DomainError("version_conflict", "analysis digest changed")
            if analysis["source_revision"] != campaign["revision"]:
                raise DomainError("stale_analysis", "analysis no longer matches source revision")
            if not analysis["body"]["quality"]["passed"]:
                raise DomainError("quality_failed", "quality gates prevent review")
            existing = db.execute("SELECT * FROM reviews WHERE campaign_id=? AND analysis_id=? AND reviewer=?",
                                  (campaign_id, analysis_id, reviewer)).fetchone()
            result = {"campaign_id": campaign_id, "analysis_id": analysis_id,
                      "reviewer": reviewer, "decision": decision,
                      "note": note.strip(), "result_sha": expected_sha}
            if existing:
                if existing["decision"] == decision and existing["note"] == note.strip() and existing["result_sha"] == expected_sha:
                    return {**result, "replayed": True}
                raise DomainError("review_conflict", "reviewer has already made a different decision")
            now = utc_now()
            db.execute("INSERT INTO reviews(campaign_id,analysis_id,reviewer,decision,note,result_sha,created_at)"
                       " VALUES(?,?,?,?,?,?,?)", (campaign_id, analysis_id, reviewer, decision,
                                                  note.strip(), expected_sha, now))
            self.store.audit(db, campaign_id, "analysis_reviewed", reviewer, analysis_id,
                             None, {**result, "created_at": now})
            return {**result, "created_at": now, "replayed": False}

    def publish(self, campaign_id: str, analysis_id: str, release_id: str,
                actor: str, expected_sha: str) -> dict[str, Any]:
        require_identifier(campaign_id, "campaign_id")
        require_identifier(analysis_id, "analysis_id")
        require_identifier(release_id, "release_id")
        require_identifier(actor, "actor")
        with self.store.transaction() as db:
            existing = db.execute("SELECT * FROM releases WHERE campaign_id=? AND release_id=?",
                                  (campaign_id, release_id)).fetchone()
            if existing:
                if existing["analysis_id"] == analysis_id and existing["result_sha"] == expected_sha:
                    return {**json.loads(existing["body"]), "replayed": True}
                raise DomainError("release_conflict", "release id already names different content")
            analysis = self.store.get_analysis(campaign_id, analysis_id, db)
            campaign = self.store.get_campaign(campaign_id, db)
            if analysis["result_sha"] != expected_sha:
                raise DomainError("version_conflict", "analysis digest changed")
            if analysis["source_revision"] != campaign["revision"]:
                raise DomainError("stale_analysis", "analysis no longer matches source revision")
            if not analysis["body"]["quality"]["passed"]:
                raise DomainError("quality_failed", "quality gates prevent publication")
            decisions = db.execute("SELECT reviewer,decision,note,result_sha FROM reviews"
                                   " WHERE campaign_id=? AND analysis_id=? ORDER BY reviewer",
                                   (campaign_id, analysis_id)).fetchall()
            if any(row["decision"] == "reject" for row in decisions):
                raise DomainError("review_rejected", "a reviewer rejected the analysis")
            approved = [dict(row) for row in decisions if row["decision"] == "approve" and row["result_sha"] == expected_sha]
            if len(approved) < 2:
                raise DomainError("reviews_missing", "two independent approvals are required")
            if actor in {row["reviewer"] for row in approved}:
                raise DomainError("self_publish", "publisher cannot be an approving reviewer")
            now = utc_now()
            result = {"campaign_id": campaign_id, "release_id": release_id,
                      "analysis_id": analysis_id, "source_revision": analysis["source_revision"],
                      "result_sha": expected_sha, "approved_by": [row["reviewer"] for row in approved],
                      "published_by": actor, "created_at": now, "analysis": analysis["body"],
                      "replayed": False}
            result["release_sha"] = digest(result)
            db.execute("INSERT INTO releases(campaign_id,release_id,analysis_id,source_revision,"
                       "result_sha,body,created_at) VALUES(?,?,?,?,?,?,?)",
                       (campaign_id, release_id, analysis_id, analysis["source_revision"],
                        expected_sha, canonical(result), now))
            self.store.audit(db, campaign_id, "analysis_published", actor, release_id,
                             None, {"analysis_id": analysis_id, "release_sha": result["release_sha"]})
            return result

    def list_reviews(self, campaign_id: str, analysis_id: str) -> list[dict[str, Any]]:
        with self.store.connect() as db:
            self.store.get_analysis(campaign_id, analysis_id, db)
            rows = db.execute("SELECT reviewer,decision,note,result_sha,created_at FROM reviews"
                              " WHERE campaign_id=? AND analysis_id=? ORDER BY reviewer",
                              (campaign_id, analysis_id)).fetchall()
            return [dict(row) for row in rows]

    def list_releases(self, campaign_id: str) -> list[dict[str, Any]]:
        with self.store.connect() as db:
            self.store.get_campaign(campaign_id, db)
            rows = db.execute("SELECT release_id,analysis_id,source_revision,result_sha,created_at"
                              " FROM releases WHERE campaign_id=? ORDER BY created_at DESC",
                              (campaign_id,)).fetchall()
            return [dict(row) for row in rows]
