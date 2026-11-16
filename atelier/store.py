from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import json
import sqlite3
from typing import Any, Iterator

from .domain import CampaignSpec, DomainError, canonical, digest, utc_now

SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS campaigns (
    id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS specs (
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    version INTEGER NOT NULL,
    body TEXT NOT NULL,
    content_sha TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(campaign_id, version)
);
CREATE TABLE IF NOT EXISTS batches (
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    batch_id TEXT NOT NULL,
    payload_sha TEXT NOT NULL,
    spec_version INTEGER NOT NULL,
    accepted INTEGER NOT NULL,
    rejected INTEGER NOT NULL,
    body TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(campaign_id, batch_id)
);
CREATE TABLE IF NOT EXISTS samples (
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    sample_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    event_time TEXT NOT NULL,
    spec_version INTEGER NOT NULL,
    measurements TEXT NOT NULL,
    content_sha TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(campaign_id, sample_id)
);
CREATE INDEX IF NOT EXISTS idx_samples_window ON samples(campaign_id,event_time,group_id,status);
CREATE TABLE IF NOT EXISTS rejections (
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    batch_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    sample_id TEXT,
    reason TEXT NOT NULL,
    detail TEXT NOT NULL,
    event_time TEXT,
    PRIMARY KEY(campaign_id,batch_id,ordinal)
);
CREATE TABLE IF NOT EXISTS audits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    kind TEXT NOT NULL,
    actor TEXT NOT NULL,
    subject TEXT NOT NULL,
    before_json TEXT,
    after_json TEXT,
    at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audits_campaign ON audits(campaign_id,id);
CREATE TABLE IF NOT EXISTS analyses (
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    analysis_id TEXT NOT NULL,
    request_sha TEXT NOT NULL,
    source_revision INTEGER NOT NULL,
    body TEXT NOT NULL,
    result_sha TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(campaign_id,analysis_id)
);
CREATE TABLE IF NOT EXISTS reviews (
    campaign_id TEXT NOT NULL,
    analysis_id TEXT NOT NULL,
    reviewer TEXT NOT NULL,
    decision TEXT NOT NULL,
    note TEXT NOT NULL,
    result_sha TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(campaign_id,analysis_id,reviewer),
    FOREIGN KEY(campaign_id,analysis_id) REFERENCES analyses(campaign_id,analysis_id)
);
CREATE TABLE IF NOT EXISTS releases (
    campaign_id TEXT NOT NULL,
    release_id TEXT NOT NULL,
    analysis_id TEXT NOT NULL,
    source_revision INTEGER NOT NULL,
    result_sha TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(campaign_id,release_id),
    FOREIGN KEY(campaign_id,analysis_id) REFERENCES analyses(campaign_id,analysis_id)
);
"""


class ClosingConnection(sqlite3.Connection):
    def __exit__(self, error_type, error, traceback):
        try:
            return super().__exit__(error_type, error, traceback)
        finally:
            self.close()


class Store:
    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript(SCHEMA)

    def connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=20, isolation_level=None, factory=ClosingConnection)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=20000")
        return db

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        db = self.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def one(db: sqlite3.Connection, sql: str, params: tuple = ()) -> sqlite3.Row:
        row = db.execute(sql, params).fetchone()
        if row is None:
            raise DomainError("not_found", "requested resource does not exist")
        return row

    def create_campaign(self, spec: CampaignSpec, actor: str) -> dict[str, Any]:
        body = spec.to_dict()
        now = utc_now()
        with self.transaction() as db:
            if db.execute("SELECT 1 FROM campaigns WHERE id=?", (spec.campaign_id,)).fetchone():
                raise DomainError("conflict", "campaign already exists")
            db.execute("INSERT INTO campaigns(id,version,revision,created_at,updated_at) VALUES(?,?,?,?,?)",
                       (spec.campaign_id, 1, 0, now, now))
            db.execute("INSERT INTO specs(campaign_id,version,body,content_sha,created_at) VALUES(?,?,?,?,?)",
                       (spec.campaign_id, 1, canonical(body), digest(body), now))
            self.audit(db, spec.campaign_id, "campaign_created", actor, spec.campaign_id, None, body)
        return body

    def get_campaign(self, campaign_id: str, db: sqlite3.Connection | None = None) -> dict[str, Any]:
        if db is None:
            with self.connect() as local:
                return self.get_campaign(campaign_id, local)
        row = self.one(db, "SELECT * FROM campaigns WHERE id=?", (campaign_id,))
        spec = self.one(db, "SELECT body FROM specs WHERE campaign_id=? AND version=?",
                        (campaign_id, row["version"]))
        return {"spec": json.loads(spec["body"]), "revision": row["revision"],
                "created_at": row["created_at"], "updated_at": row["updated_at"]}

    def get_spec(self, campaign_id: str, version: int | None = None,
                 db: sqlite3.Connection | None = None) -> CampaignSpec:
        if db is None:
            with self.connect() as local:
                return self.get_spec(campaign_id, version, local)
        current = self.get_campaign(campaign_id, db)
        number = current["spec"]["version"] if version is None else version
        row = self.one(db, "SELECT body FROM specs WHERE campaign_id=? AND version=?", (campaign_id, number))
        return CampaignSpec.from_dict(json.loads(row["body"]), number)

    def update_spec(self, campaign_id: str, supplied: dict[str, Any], expected_version: int,
                    actor: str) -> dict[str, Any]:
        with self.transaction() as db:
            current = self.get_campaign(campaign_id, db)
            previous = current["spec"]
            if previous["version"] != expected_version:
                raise DomainError("version_conflict", "campaign specification version changed")
            next_spec = CampaignSpec.from_dict({**supplied, "campaign_id": campaign_id}, expected_version + 1)
            if not set(previous["groups"]).issubset(next_spec.groups):
                raise DomainError("incompatible_spec", "existing groups cannot be removed")
            old_rules = {rule["name"]: rule for rule in previous["metrics"]}
            new_rules = {rule.name: rule for rule in next_spec.rules}
            for name, old in old_rules.items():
                if name not in new_rules or old["unit"] != new_rules[name].unit:
                    raise DomainError("incompatible_spec", "existing metric names and units cannot change")
            body = next_spec.to_dict()
            now = utc_now()
            db.execute("INSERT INTO specs(campaign_id,version,body,content_sha,created_at) VALUES(?,?,?,?,?)",
                       (campaign_id, next_spec.version, canonical(body), digest(body), now))
            db.execute("UPDATE campaigns SET version=?, revision=revision+1, updated_at=? WHERE id=?",
                       (next_spec.version, now, campaign_id))
            self.audit(db, campaign_id, "spec_updated", actor, str(next_spec.version), previous, body)
            return body

    @staticmethod
    def audit(db: sqlite3.Connection, campaign_id: str, kind: str, actor: str,
              subject: str, before: Any, after: Any) -> None:
        db.execute("INSERT INTO audits(campaign_id,kind,actor,subject,before_json,after_json,at) VALUES(?,?,?,?,?,?,?)",
                   (campaign_id, kind, actor, subject, canonical(before), canonical(after), utc_now()))

    @staticmethod
    def bump(db: sqlite3.Connection, campaign_id: str) -> int:
        db.execute("UPDATE campaigns SET revision=revision+1,updated_at=? WHERE id=?", (utc_now(), campaign_id))
        return db.execute("SELECT revision FROM campaigns WHERE id=?", (campaign_id,)).fetchone()[0]

    def list_samples(self, campaign_id: str, *, status: str | None = None,
                     start: str | None = None, end: str | None = None,
                     db: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
        if db is None:
            with self.connect() as local:
                return self.list_samples(campaign_id, status=status, start=start, end=end, db=local)
        self.get_campaign(campaign_id, db)
        clauses = ["campaign_id=?"]
        params: list[Any] = [campaign_id]
        for expression, value in (("status=?", status), ("event_time>=?", start), ("event_time<?", end)):
            if value is not None:
                clauses.append(expression)
                params.append(value)
        rows = db.execute("SELECT * FROM samples WHERE " + " AND ".join(clauses) +
                          " ORDER BY event_time,sample_id", params).fetchall()
        return [{**dict(row), "measurements": json.loads(row["measurements"])} for row in rows]

    def list_audits(self, campaign_id: str, since_id: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as db:
            self.get_campaign(campaign_id, db)
            rows = db.execute("SELECT * FROM audits WHERE campaign_id=? AND id>? ORDER BY id LIMIT ?",
                              (campaign_id, since_id, limit)).fetchall()
            return [{**dict(row), "before": json.loads(row["before_json"]),
                     "after": json.loads(row["after_json"])} for row in rows]

    def get_batch(self, campaign_id: str, batch_id: str) -> dict[str, Any]:
        with self.connect() as db:
            row = self.one(db, "SELECT * FROM batches WHERE campaign_id=? AND batch_id=?",
                           (campaign_id, batch_id))
            return {**dict(row), "body": json.loads(row["body"])}

    def get_analysis(self, campaign_id: str, analysis_id: str,
                     db: sqlite3.Connection | None = None) -> dict[str, Any]:
        if db is None:
            with self.connect() as local:
                return self.get_analysis(campaign_id, analysis_id, local)
        row = self.one(db, "SELECT * FROM analyses WHERE campaign_id=? AND analysis_id=?",
                       (campaign_id, analysis_id))
        return {**dict(row), "body": json.loads(row["body"])}

    def get_release(self, campaign_id: str, release_id: str) -> dict[str, Any]:
        with self.connect() as db:
            row = self.one(db, "SELECT * FROM releases WHERE campaign_id=? AND release_id=?",
                           (campaign_id, release_id))
            return {**dict(row), "body": json.loads(row["body"])}
