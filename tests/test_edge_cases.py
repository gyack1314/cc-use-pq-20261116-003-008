import json
from pathlib import Path
import tempfile
import unittest

from atelier.analysis import Analyzer, statistics
from atelier.domain import CampaignSpec, DomainError, MetricRule, decimal, parse_time
from atelier.evidence import EvidenceService
from atelier.explore import Explorer
from atelier.ingest import Ingestor
from atelier.inspection import Inspector
from atelier.lifecycle import Lifecycle
from atelier.query import QueryService
from atelier.reconcile import Reconciler
from atelier.store import Store


def synthetic():
    return json.loads((Path(__file__).parents[1] / "fixtures" / "synthetic_experiment.json").read_text())


class EdgeCaseTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = Store(Path(self.directory.name) / "cases.sqlite3")
        self.fixture = synthetic()
        self.spec = CampaignSpec.from_dict(self.fixture["campaign"])
        self.store.create_campaign(self.spec, "operator")
        self.ingest = Ingestor(self.store)
        self.analyzer = Analyzer(self.store)
        self.id = self.spec.campaign_id
        self.window = self.fixture["window"]

    def add(self, samples=None, batch="batch_one"):
        return self.ingest.ingest(self.id, batch,
                                  self.fixture["batches"][0]["samples"] if samples is None else samples,
                                  "operator")

    def analyze(self, analysis="analysis_one"):
        return self.analyzer.build(self.id, analysis, self.window["start"], self.window["end"], "analyst")

    def test_time_requires_zone(self):
        with self.assertRaises(DomainError) as caught:
            parse_time("2026-01-01T00:00:00")
        self.assertEqual(caught.exception.code, "invalid_time")
        self.assertEqual(parse_time("2026-01-01T08:00:00+08:00"),
                         "2026-01-01T00:00:00.000000+00:00")

    def test_decimal_rejects_nonfinite_and_boolean(self):
        for value in ("NaN", "Infinity", True):
            with self.subTest(value=value), self.assertRaises(DomainError):
                decimal(value, "metric")

    def test_rule_rejects_excess_precision(self):
        rule = MetricRule.from_dict({"name": "test_metric", "unit": "percent", "minimum": "0",
                                     "maximum": "100", "precision": 2})
        self.assertEqual(rule.check("10.25"), "10.25")
        with self.assertRaises(DomainError) as caught:
            rule.check("10.255")
        self.assertEqual(caught.exception.code, "excess_precision")

    def test_spec_rejects_duplicate_metric_names(self):
        body = dict(self.fixture["campaign"])
        body["metrics"] = body["metrics"] * 2
        with self.assertRaises(DomainError) as caught:
            CampaignSpec.from_dict(body)
        self.assertEqual(caught.exception.code, "invalid_metrics")

    def test_spec_rejects_duplicate_group_names(self):
        body = dict(self.fixture["campaign"])
        body["groups"] = ["control", "control"]
        with self.assertRaises(DomainError) as caught:
            CampaignSpec.from_dict(body)
        self.assertEqual(caught.exception.code, "invalid_groups")

    def test_optional_metric_absence_is_retained(self):
        body = dict(self.fixture["campaign"])
        body["metrics"] = body["metrics"] + [{"name": "mass_g", "unit": "g",
                                              "minimum": "0", "maximum": "10", "required": False}]
        changed = self.store.update_spec(self.id, body, 1, "operator")
        self.assertEqual(changed["version"], 2)
        result = self.add()
        self.assertEqual(len(result["accepted"]), 4)
        coverage = Inspector(self.store).coverage(self.id, self.window["start"], self.window["end"])
        self.assertEqual(coverage["groups"]["control"]["metrics"]["mass_g"]["missing"], 2)

    def test_unknown_measurement_is_rejected(self):
        sample = dict(self.fixture["batches"][0]["samples"][0])
        sample["measurements"] = {"not_declared": "1"}
        result = self.add([sample])
        self.assertEqual(result["rejected"][0]["reason"], "unknown_metric")
        self.assertEqual(result["accepted"], [])

    def test_invalid_time_does_not_count_in_window_rejections(self):
        sample = dict(self.fixture["batches"][0]["samples"][0])
        sample["event_time"] = "not-a-time"
        self.add([sample])
        quality = self.analyze()["quality"]
        self.assertEqual(quality["rejected_count"], 0)
        self.assertEqual(quality["gates"][0], "no_samples")

    def test_rejection_outside_window_not_in_budget(self):
        rows = list(self.fixture["batches"][0]["samples"])
        outside = {**rows[0], "sample_id": "outside_sample", "event_time": "2026-01-03T01:00:00Z",
                   "measurements": {"yield_pct": "999"}}
        self.add(rows + [outside])
        self.assertEqual(self.analyze()["quality"]["rejected_count"], 0)

    def test_analysis_window_size_is_limited(self):
        self.add()
        with self.assertRaises(DomainError) as caught:
            self.analyzer.build(self.id, "too_long", "2026-01-01T00:00:00Z",
                                "2026-01-03T00:00:00Z", "analyst")
        self.assertEqual(caught.exception.code, "invalid_window")

    def test_negative_baseline_mean_uses_absolute_denominator(self):
        body = dict(self.fixture["campaign"])
        body["metrics"] = [{"name": "signed_gap", "unit": "unitless", "minimum": "-100",
                            "maximum": "100", "precision": 2}]
        body["campaign_id"] = "signed_trial"
        self.store.create_campaign(CampaignSpec.from_dict(body), "operator")
        rows = []
        for group, values in (("control", ["-10", "-10"]), ("process_b", ["-5", "-5"])):
            for index, value in enumerate(values):
                rows.append({"sample_id": group + "_" + str(index), "group_id": group,
                             "event_time": "2026-01-01T01:00:00Z", "measurements": {"signed_gap": value}})
        Ingestor(self.store).ingest("signed_trial", "batch_signed", rows, "operator")
        result = Analyzer(self.store).build("signed_trial", "signed_analysis",
                                            self.window["start"], self.window["end"], "analyst")
        self.assertEqual(result["contrasts"]["process_b"]["signed_gap"]["relative_percent"], "50.0")

    def test_distribution_and_empty_pair(self):
        self.add()
        explorer = Explorer(self.store)
        distribution = explorer.distribution(self.id, "yield_pct", self.window["start"], self.window["end"])
        self.assertEqual(distribution["groups"]["control"]["p10"], "71.300")
        with self.assertRaises(DomainError) as caught:
            explorer.paired_metrics(self.id, "yield_pct", "yield_pct",
                                    self.window["start"], self.window["end"])
        self.assertEqual(caught.exception.code, "invalid_pair")

    def test_query_filters_and_validation(self):
        self.add()
        query = QueryService(self.store)
        filtered = query.samples(self.id, group_id="process_b", metric="yield_pct", minimum="79")
        self.assertEqual([row["sample_id"] for row in filtered["items"]], ["sample_d"])
        with self.assertRaises(DomainError) as caught:
            query.samples(self.id, metric="yield_pct", minimum="90", maximum="10")
        self.assertEqual(caught.exception.code, "invalid_range")
        with self.assertRaises(DomainError):
            query.samples(self.id, limit=0)

    def test_batch_pagination_and_audit_cursor(self):
        self.add(self.fixture["batches"][0]["samples"][:1])
        self.add(self.fixture["batches"][0]["samples"][1:], "batch_two")
        query = QueryService(self.store)
        first = query.batches(self.id, limit=1)
        self.assertEqual(first["next_after"], "batch_one")
        second = query.batches(self.id, after=first["next_after"], limit=1)
        self.assertEqual(second["items"][0]["batch_id"], "batch_two")
        audits = query.audits(self.id, kind="batch_ingested", limit=1)
        self.assertEqual(len(audits["items"]), 1)
        self.assertIsNotNone(audits["next_after_id"])

    def test_evidence_and_reconciliation_after_release(self):
        from atelier.release import Publisher
        self.add()
        analysis = self.analyze()
        publisher = Publisher(self.store)
        for reviewer in ("reviewer_a", "reviewer_b"):
            publisher.review(self.id, "analysis_one", reviewer, "approve",
                             "Measurements independently checked", analysis["result_sha"])
        publisher.publish(self.id, "analysis_one", "release_one", "publisher", analysis["result_sha"])
        self.assertTrue(EvidenceService(self.store).verify_campaign(self.id)["ok"])
        self.assertTrue(Reconciler(self.store).full_report(self.id)["ok"])
        self.assertEqual(Lifecycle(self.store).inventory(self.id)["state_counts"], {"published": 1})

    def test_tampered_analysis_is_detected(self):
        self.add()
        self.analyze()
        with self.store.transaction() as db:
            db.execute("UPDATE analyses SET result_sha=? WHERE campaign_id=? AND analysis_id=?",
                       ("0" * 64, self.id, "analysis_one"))
        verification = EvidenceService(self.store).verify_campaign(self.id)
        self.assertFalse(verification["ok"])
        self.assertEqual(verification["issues"][0]["kind"], "analysis_digest")

    def test_tampered_batch_count_is_detected(self):
        self.add()
        with self.store.transaction() as db:
            db.execute("UPDATE batches SET accepted=99 WHERE campaign_id=? AND batch_id=?",
                       (self.id, "batch_one"))
        report = Reconciler(self.store).sample_counts(self.id)
        self.assertFalse(report["matches"])
        self.assertEqual(report["batches"][0]["actual_accepted"], 4)

    def test_lifecycle_marks_stale_after_correction(self):
        self.add()
        self.analyze()
        sample = self.store.list_samples(self.id)[0]
        replacement = {"sample_id": sample["sample_id"], "group_id": sample["group_id"],
                       "event_time": sample["event_time"], "measurements": {"yield_pct": "70.00"}}
        self.ingest.correct(self.id, sample["sample_id"], replacement, sample["content_sha"], "operator")
        state = Lifecycle(self.store).inventory(self.id)
        self.assertEqual(state["analyses"][0]["state"], "stale")


if __name__ == "__main__":
    unittest.main()
