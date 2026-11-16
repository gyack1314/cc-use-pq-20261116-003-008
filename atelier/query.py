from __future__ import annotations

from decimal import Decimal
import json
from typing import Any

from .domain import DomainError, decimal, parse_time, require_identifier, require_metric
from .store import Store


class QueryService:
    def __init__(self, store: Store):
        self.store = store

    def samples(self, campaign_id: str, *, group_id: str | None = None,
                status: str | None = None, metric: str | None = None,
                minimum: Any = None, maximum: Any = None,
                after: str | None = None, limit: int = 100) -> dict[str, Any]:
        require_identifier(campaign_id, "campaign_id")
        if group_id is not None:
            require_identifier(group_id, "group_id")
        if metric is not None:
            require_metric(metric)
        if status is not None and status not in {"active", "void"}:
            raise DomainError("invalid_status", "status must be active or void")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 500:
            raise DomainError("invalid_limit", "limit must be between 1 and 500")
        low = decimal(minimum, "minimum") if minimum is not None else None
        high = decimal(maximum, "maximum") if maximum is not None else None
        if low is not None and high is not None and low > high:
            raise DomainError("invalid_range", "minimum exceeds maximum")
        if (low is not None or high is not None) and metric is None:
            raise DomainError("missing_metric", "measurement range requires metric")
        with self.store.connect() as db:
            spec = self.store.get_spec(campaign_id, db=db)
            if group_id is not None and group_id not in spec.groups:
                raise DomainError("unknown_group", "group is not declared")
            if metric is not None and metric not in {rule.name for rule in spec.rules}:
                raise DomainError("unknown_metric", "metric is not declared")
            clauses = ["campaign_id=?"]
            params: list[Any] = [campaign_id]
            if group_id is not None:
                clauses.append("group_id=?")
                params.append(group_id)
            if status is not None:
                clauses.append("status=?")
                params.append(status)
            if after is not None:
                require_identifier(after, "after")
                clauses.append("sample_id>?")
                params.append(after)
            rows = db.execute("SELECT * FROM samples WHERE " + " AND ".join(clauses)
                              + " ORDER BY sample_id", params).fetchall()
            selected = []
            for row in rows:
                measurements = json.loads(row["measurements"])
                if metric is not None:
                    value = measurements.get(metric)
                    if value is None:
                        continue
                    number = Decimal(value)
                    if low is not None and number < low:
                        continue
                    if high is not None and number > high:
                        continue
                selected.append({**dict(row), "measurements": measurements})
                if len(selected) > limit:
                    break
            next_after = selected[limit - 1]["sample_id"] if len(selected) > limit else None
            return {"campaign_id": campaign_id, "items": selected[:limit],
                    "next_after": next_after}

    def batches(self, campaign_id: str, *, after: str | None = None,
                limit: int = 100) -> dict[str, Any]:
        require_identifier(campaign_id, "campaign_id")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 500:
            raise DomainError("invalid_limit", "limit must be between 1 and 500")
        with self.store.connect() as db:
            self.store.get_campaign(campaign_id, db)
            if after is not None:
                require_identifier(after, "after")
            rows = db.execute("SELECT batch_id,payload_sha,spec_version,accepted,rejected,created_at"
                              " FROM batches WHERE campaign_id=? AND batch_id>? ORDER BY batch_id LIMIT ?",
                              (campaign_id, after or "", limit + 1)).fetchall()
            items = [dict(row) for row in rows[:limit]]
            return {"campaign_id": campaign_id, "items": items,
                    "next_after": items[-1]["batch_id"] if len(rows) > limit else None}

    def audits(self, campaign_id: str, *, kind: str | None = None,
               after_id: int = 0, limit: int = 100) -> dict[str, Any]:
        require_identifier(campaign_id, "campaign_id")
        if kind is not None:
            require_identifier(kind, "kind")
        if not isinstance(after_id, int) or after_id < 0:
            raise DomainError("invalid_cursor", "after_id must be nonnegative")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 500:
            raise DomainError("invalid_limit", "limit must be between 1 and 500")
        with self.store.connect() as db:
            self.store.get_campaign(campaign_id, db)
            clauses = ["campaign_id=?", "id>?"]
            params: list[Any] = [campaign_id, after_id]
            if kind is not None:
                clauses.append("kind=?")
                params.append(kind)
            rows = db.execute("SELECT id,kind,actor,subject,at FROM audits WHERE "
                              + " AND ".join(clauses) + " ORDER BY id LIMIT ?", params + [limit + 1]).fetchall()
            items = [dict(row) for row in rows[:limit]]
            return {"campaign_id": campaign_id, "items": items,
                    "next_after_id": items[-1]["id"] if len(rows) > limit else None}
