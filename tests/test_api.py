import json
from pathlib import Path
import tempfile
import unittest

from fastapi.testclient import TestClient

from atelier.api import create_app


def fixture():
    return json.loads((Path(__file__).parents[1] / "fixtures" / "synthetic_experiment.json").read_text())


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.client = TestClient(create_app(str(Path(self.folder.name) / "api.sqlite3")))
        self.addCleanup(self.client.close)
        self.data = fixture()
        spec = {**self.data["campaign"], "actor": "operator"}
        response = self.client.post("/campaigns", json=spec)
        self.assertEqual(response.status_code, 201, response.text)
        self.base = "/campaigns/coating_trial"

    def upload(self, samples=None, batch="batch_one"):
        return self.client.post(self.base + "/batches/" + batch,
                                json={"actor": "operator", "samples":
                                      samples if samples is not None else self.data["batches"][0]["samples"]})

    def analyze(self, analysis="analysis_one"):
        return self.client.post(self.base + "/analyses/" + analysis,
                                json={**self.data["window"], "actor": "analyst"})

    def test_health_and_spec_readback(self):
        self.assertEqual(self.client.get("/health").json(), {"status": "ok"})
        response = self.client.get(self.base)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["spec"]["version"], 1)
        self.assertEqual(response.json()["revision"], 0)

    def test_complete_http_release_flow(self):
        self.assertEqual(self.upload().status_code, 200)
        analysis = self.analyze().json()
        self.assertTrue(analysis["quality"]["passed"])
        for reviewer in ("reviewer_a", "reviewer_b"):
            response = self.client.post(self.base + "/analyses/analysis_one/reviews",
                                        json={"reviewer": reviewer, "decision": "approve",
                                              "note": "Measurements checked in ledger",
                                              "expected_sha": analysis["result_sha"]})
            self.assertEqual(response.status_code, 200, response.text)
        released = self.client.post(self.base + "/releases/release_one",
                                    json={"analysis_id": "analysis_one", "actor": "publisher",
                                          "expected_sha": analysis["result_sha"]})
        self.assertEqual(released.status_code, 200, released.text)
        self.assertEqual(released.json()["approved_by"], ["reviewer_a", "reviewer_b"])
        self.assertEqual(self.client.get(self.base + "/releases/release_one").json()["release_sha"],
                         released.json()["release_sha"])
        self.assertEqual(self.client.get(self.base + "/reconciliation").json()["ok"], True)
        self.assertEqual(self.client.get(self.base + "/evidence/verify").json()["ok"], True)

    def test_invalid_campaign_returns_business_error(self):
        bad = {**self.data["campaign"], "campaign_id": "No Uppercase", "actor": "operator"}
        response = self.client.post("/campaigns", json=bad)
        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["error"], "invalid_identifier")

    def test_unknown_campaign_returns_not_found(self):
        response = self.client.get("/campaigns/unknown_campaign")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["error"], "not_found")

    def test_batch_replay_and_conflict(self):
        first = self.upload()
        second = self.upload()
        self.assertFalse(first.json()["replayed"])
        self.assertTrue(second.json()["replayed"])
        different = self.upload(samples=self.data["batches"][0]["samples"][:1])
        self.assertEqual(different.status_code, 409)
        self.assertEqual(different.json()["error"], "batch_conflict")

    def test_rejection_report_and_trend(self):
        bad = {**self.data["batches"][0]["samples"][0], "sample_id": "bad_sample",
               "measurements": {"yield_pct": "200"}}
        self.upload(samples=self.data["batches"][0]["samples"] + [bad])
        report = self.client.get(self.base + "/batches/batch_one/report").json()
        self.assertEqual(report["recorded_accepted"], 4)
        self.assertEqual(report["recorded_rejected"], 1)
        self.assertEqual(report["rejected"][0]["reason"], "out_of_range")
        trend = self.client.get(self.base + "/rejection-trends").json()
        self.assertEqual(trend["batches"][0]["reasons"]["out_of_range"], 1)

    def test_coverage_and_distribution(self):
        self.upload()
        params = self.data["window"]
        coverage = self.client.get(self.base + "/coverage", params=params).json()
        self.assertEqual(coverage["groups"]["control"]["samples"], 2)
        self.assertEqual(coverage["groups"]["process_b"]["metrics"]["yield_pct"]["covered"], 2)
        distribution = self.client.get(self.base + "/distribution/yield_pct", params=params).json()
        self.assertEqual(distribution["groups"]["control"]["statistics"]["mean"], "71.70")
        self.assertEqual(distribution["groups"]["process_b"]["p25"], "78.7500")

    def test_query_page_cursor(self):
        self.upload()
        first = self.client.get(self.base + "/sample-query", params={"limit": 2}).json()
        self.assertEqual(len(first["items"]), 2)
        self.assertEqual(first["next_after"], "sample_b")
        second = self.client.get(self.base + "/sample-query",
                                 params={"limit": 2, "after": first["next_after"]}).json()
        self.assertEqual(len(second["items"]), 2)
        self.assertIsNone(second["next_after"])
        filtered = self.client.get(self.base + "/sample-query",
                                   params={"metric": "yield_pct", "minimum": "78"}).json()
        self.assertEqual([row["sample_id"] for row in filtered["items"]], ["sample_c", "sample_d"])

    def test_invalid_query_filter_returns_error(self):
        response = self.client.get(self.base + "/sample-query", params={"minimum": "1"})
        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["error"], "missing_metric")
        response = self.client.get(self.base + "/sample-query", params={"group_id": "unknown_group"})
        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["error"], "unknown_group")

    def test_analysis_comparison_after_correction(self):
        self.upload()
        self.analyze()
        sample = self.client.get(self.base + "/samples").json()[0]
        replacement = {"sample_id": sample["sample_id"], "group_id": sample["group_id"],
                       "event_time": sample["event_time"], "measurements": {"yield_pct": "70.00"}}
        response = self.client.put(self.base + "/samples/" + sample["sample_id"],
                                   json={"replacement": replacement, "expected_sha": sample["content_sha"],
                                         "actor": "operator"})
        self.assertEqual(response.status_code, 200)
        self.analyze("analysis_two")
        comparison = self.client.get(self.base + "/analyses/analysis_one/compare/analysis_two").json()
        self.assertEqual(comparison["revision_after"] - comparison["revision_before"], 1)
        self.assertIn("yield_pct", comparison["metrics_changed"]["control"])

    def test_stale_review_is_blocked_after_void(self):
        self.upload()
        analysis = self.analyze().json()
        sample = self.client.get(self.base + "/samples").json()[0]
        response = self.client.post(self.base + "/samples/" + sample["sample_id"] + "/void",
                                    json={"expected_sha": sample["content_sha"],
                                          "reason": "Device calibration was invalid", "actor": "operator"})
        self.assertEqual(response.status_code, 200)
        self.assertFalse(self.client.get(self.base + "/analyses/analysis_one/status").json()["current"])
        review = self.client.post(self.base + "/analyses/analysis_one/reviews",
                                  json={"reviewer": "reviewer_a", "decision": "approve",
                                        "note": "Measurements checked in ledger",
                                        "expected_sha": analysis["result_sha"]})
        self.assertEqual(review.status_code, 409)
        self.assertEqual(review.json()["error"], "stale_analysis")

    def test_evidence_export_has_matching_digest(self):
        self.upload()
        self.analyze()
        output = self.client.get(self.base + "/evidence/export")
        self.assertEqual(output.status_code, 200, output.text)
        body = output.json()
        self.assertTrue(body["verification"]["ok"])
        self.assertEqual(len(body["digest"]), 64)
        self.assertEqual(len(body["payload"]["samples"]), 4)

    def test_audit_and_batch_pagination(self):
        self.upload()
        self.analyze()
        batches = self.client.get(self.base + "/batch-query", params={"limit": 1}).json()
        self.assertEqual(batches["items"][0]["batch_id"], "batch_one")
        audits = self.client.get(self.base + "/audits", params={"limit": 2}).json()
        self.assertEqual(len(audits), 2)
        self.assertLess(audits[0]["id"], audits[1]["id"])


if __name__ == "__main__":
    unittest.main()
