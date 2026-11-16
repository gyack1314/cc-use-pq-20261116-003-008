from pathlib import Path
import tempfile
import unittest

from fastapi.testclient import TestClient

from atelier.api import create_app


def sample(sample_id, group, hour, day, value):
    return {"sample_id": sample_id, "group_id": group,
            "event_time": f"2026-01-0{day}T{hour:02d}:00:00Z",
            "measurements": {"yield_pct": value}}


class AlertApiTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.client = TestClient(create_app(str(Path(self.folder.name) / "api.sqlite3")))
        self.addCleanup(self.client.close)
        campaign = {
            "campaign_id": "coating_trial", "title": "Synthetic coating experiment",
            "groups": ["control", "process_b"], "baseline": "control",
            "metrics": [{"name": "yield_pct", "unit": "percent", "minimum": "0",
                         "maximum": "100", "precision": 2}],
            "min_samples": 2, "max_bad_fraction": "0.2", "window_hours": 24,
            "actor": "operator"}
        response = self.client.post("/campaigns", json=campaign)
        self.assertEqual(response.status_code, 201, response.text)
        self.base = "/campaigns/coating_trial"
        for day in (1, 2, 3):
            rows = [
                sample(f"c{day}_0", "control", 1, day, "71.20"),
                sample(f"c{day}_1", "control", 2, day, "72.20"),
                sample(f"t{day}_0", "process_b", 3, day, "78.50"),
                sample(f"t{day}_1", "process_b", 4, day, "79.50"),
            ]
            uploaded = self.client.post(f"{self.base}/batches/batch_day{day}",
                                        json={"actor": "operator", "samples": rows})
            self.assertEqual(uploaded.status_code, 200, uploaded.text)

    def monitor(self):
        response = self.client.post(self.base + "/monitors/trend_one", json={
            "actor": "process_owner", "group_id": "process_b", "metric": "yield_pct",
            "deviation_percent": "5", "persistence": 2, "window_count": 3,
            "anchor": "2026-01-01T00:00:00Z"})
        self.assertEqual(response.status_code, 201, response.text)
        return response.json()

    def evaluate(self):
        response = self.client.post(self.base + "/monitors/trend_one/evaluate",
                                    json={"actor": "process_owner"})
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def test_full_alert_lifecycle_over_http(self):
        self.monitor()
        self.assertEqual(len(self.client.get(self.base + "/monitors").json()), 1)
        judged = self.evaluate()
        self.assertEqual(judged["classification"], "persistent_deviation")
        alert_id = judged["alert_id"]
        listing = self.client.get(self.base + "/alerts", params={"state": "open"}).json()
        self.assertEqual([item["alert_id"] for item in listing["items"]], [alert_id])
        detail = self.client.get(f"{self.base}/alerts/{alert_id}").json()
        self.assertEqual(detail["state"], "open")
        self.assertEqual(len(self.client.get(f"{self.base}/alerts/{alert_id}/judgments").json()), 1)
        # Stale digest is refused.
        stale = self.client.post(f"{self.base}/alerts/{alert_id}/dispositions", json={
            "reviewer": "reviewer_a", "decision": "confirm",
            "note": "Attempting to use an outdated digest",
            "expected_judgment_sha": "0" * 64})
        self.assertEqual(stale.status_code, 409)
        self.assertEqual(stale.json()["error"], "version_conflict")
        # Confirmation binds to the current judgment.
        confirm = self.client.post(f"{self.base}/alerts/{alert_id}/dispositions", json={
            "reviewer": "reviewer_a", "decision": "confirm",
            "note": "Persistent drift confirmed against baseline",
            "expected_judgment_sha": judged["judgment_sha"]})
        self.assertEqual(confirm.status_code, 200, confirm.text)
        self.assertEqual(confirm.json()["state"], "confirmed")
        dispositions = self.client.get(f"{self.base}/alerts/{alert_id}/dispositions").json()
        self.assertEqual([item["decision"] for item in dispositions], ["confirm"])

    def test_alert_gate_is_visible_on_analysis_evidence_endpoint(self):
        self.monitor()
        judged = self.evaluate()
        analysis = self.client.post(self.base + "/analyses/analysis_one", json={
            "actor": "analyst", "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-02T00:00:00Z"})
        self.assertEqual(analysis.status_code, 200, analysis.text)
        evidence = self.client.get(self.base + "/analyses/analysis_one/alert-evidence").json()
        self.assertFalse(evidence["publishable"])
        self.assertEqual(evidence["blockers"][0]["block_reason"], "open_persistent_alert")
        self.assertEqual(len(evidence["evidence"][0]["trigger_windows"]), 3)
        # Review is blocked while the persistent alert is open.
        review = self.client.post(self.base + "/analyses/analysis_one/reviews", json={
            "reviewer": "reviewer_a", "decision": "approve",
            "note": "Measurements checked in ledger",
            "expected_sha": analysis.json()["result_sha"]})
        self.assertEqual(review.status_code, 409)
        self.assertEqual(review.json()["error"], "alert_gate_blocked")
        # Reject the alert, then the analysis can flow through review.
        rejected = self.client.post(f"{self.base}/alerts/{judged['alert_id']}/dispositions", json={
            "reviewer": "reviewer_a", "decision": "reject",
            "note": "Planned process adjustment explains the drift",
            "expected_judgment_sha": judged["judgment_sha"]})
        self.assertEqual(rejected.status_code, 200, rejected.text)
        review = self.client.post(self.base + "/analyses/analysis_one/reviews", json={
            "reviewer": "reviewer_a", "decision": "approve",
            "note": "Measurements checked in ledger",
            "expected_sha": analysis.json()["result_sha"]})
        self.assertEqual(review.status_code, 200, review.text)
        self.assertEqual(review.json()["alert_evidence"][0]["state"], "rejected")
        # Verification still passes with the alert chain present.
        verify = self.client.get(self.base + "/evidence/verify").json()
        self.assertTrue(verify["ok"])
        self.assertEqual(verify["counts"]["alerts"], 1)
        self.assertEqual(verify["counts"]["dispositions"], 1)

    def test_suspended_alert_surfaces_quality_gate_in_query(self):
        self.monitor()
        self.evaluate()
        samples = self.client.get(self.base + "/samples").json()
        target = next(row for row in samples
                      if row["group_id"] == "process_b" and row["event_time"].startswith("2026-01-01"))
        voided = self.client.post(f"{self.base}/samples/{target['sample_id']}/void", json={
            "expected_sha": target["content_sha"],
            "reason": "Late calibration failure invalidated reading", "actor": "operator"})
        self.assertEqual(voided.status_code, 200, voided.text)
        rejudged = self.evaluate()
        self.assertEqual(rejudged["state"], "suspended")
        alert_id = rejudged["alert_id"]
        detail = self.client.get(f"{self.base}/alerts/{alert_id}").json()
        failed = detail["latest_judgment"]["windows"][0]
        self.assertFalse(failed["qualified"])
        self.assertIn("insufficient_samples:process_b", failed["gates"])
        # Querying still traces the trigger window, quality reason and history.
        judgments = self.client.get(f"{self.base}/alerts/{alert_id}/judgments").json()
        self.assertEqual([item["judgment_version"] for item in judgments], [1, 2])

    def test_unknown_alert_returns_not_found(self):
        response = self.client.get(self.base + "/alerts/alert_missing")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["error"], "not_found")


if __name__ == "__main__":
    unittest.main()
