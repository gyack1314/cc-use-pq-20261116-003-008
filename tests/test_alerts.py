import json
from pathlib import Path
import tempfile
import unittest

from atelier.alerts import AlertManager, PERSISTENT, INCIDENTAL, INSUFFICIENT, NORMAL
from atelier.analysis import Analyzer
from atelier.domain import CampaignSpec, DomainError
from atelier.ingest import Ingestor
from atelier.release import Publisher
from atelier.store import Store


def synthetic():
    return json.loads((Path(__file__).parents[1] / "fixtures" / "synthetic_experiment.json").read_text())


def sample(sample_id, group, hour, day, value):
    return {"sample_id": sample_id, "group_id": group,
            "event_time": f"2026-01-0{day}T{hour:02d}:00:00Z",
            "measurements": {"yield_pct": value}}


class AlertTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = Store(Path(self.directory.name) / "alerts.sqlite3")
        self.fixture = synthetic()
        self.spec = CampaignSpec.from_dict(self.fixture["campaign"])
        self.campaign = self.spec.campaign_id
        self.store.create_campaign(self.spec, "operator")
        self.ingest = Ingestor(self.store)
        self.analyzer = Analyzer(self.store)
        self.publisher = Publisher(self.store)
        self.alerts = AlertManager(self.store)
        # Three 24h grid windows: Jan 1, Jan 2, Jan 3 (anchor aligned to midnight UTC).
        self.anchor = "2026-01-01T00:00:00Z"

    def add_day(self, day, controls, targets, batch=None):
        rows = []
        for index, value in enumerate(controls):
            rows.append(sample(f"c{day}_{index}", "control", 1 + index, day, value))
        for index, value in enumerate(targets):
            rows.append(sample(f"t{day}_{index}", "process_b", 3 + index, day, value))
        return self.ingest.ingest(
            self.campaign, batch or f"batch_day{day}", rows, "operator")

    def make_monitor(self, monitor_id="trend_one", threshold="5", persistence=2, window_count=3):
        return self.alerts.create_monitor(
            self.campaign, monitor_id,
            {"group_id": "process_b", "metric": "yield_pct",
             "deviation_percent": threshold, "persistence": persistence,
             "window_count": window_count, "anchor": self.anchor},
            "process_owner")

    def evaluate(self, monitor_id="trend_one"):
        return self.alerts.evaluate(self.campaign, monitor_id, "process_owner")

    def alert_id(self, monitor_id="trend_one"):
        return "alert-" + monitor_id

    # --- monitor management -------------------------------------------------

    def test_monitor_validation_and_sequence(self):
        body = self.make_monitor()
        self.assertEqual(body["window_count"], 3)
        self.assertEqual(body["sequence_start"], "2026-01-01T00:00:00.000000+00:00")
        self.assertEqual(body["sequence_end"], "2026-01-04T00:00:00.000000+00:00")
        self.assertEqual(len(self.alerts.list_monitors(self.campaign)), 1)
        with self.assertRaises(DomainError) as caught:
            self.alerts.create_monitor(
                self.campaign, "bad_one",
                {"group_id": "control", "metric": "yield_pct",
                 "deviation_percent": "5", "anchor": self.anchor}, "process_owner")
        self.assertEqual(caught.exception.code, "invalid_monitor")
        with self.assertRaises(DomainError) as caught:
            self.alerts.create_monitor(
                self.campaign, "bad_two",
                {"group_id": "process_b", "metric": "yield_pct",
                 "deviation_percent": "5", "persistence": 4, "window_count": 3,
                 "anchor": self.anchor}, "process_owner")
        self.assertEqual(caught.exception.code, "invalid_monitor")
        with self.assertRaises(DomainError) as caught:
            self.make_monitor()
        self.assertEqual(caught.exception.code, "conflict")

    # --- classification -----------------------------------------------------

    def test_persistent_deviation_across_consecutive_windows(self):
        # control mean 71.7; target mean 79.0 -> deviation ~10.2% in every window
        for day in (1, 2, 3):
            self.add_day(day, ["71.20", "72.20"], ["78.50", "79.50"])
        self.make_monitor()
        result = self.evaluate()
        self.assertFalse(result["replayed"])
        self.assertEqual(result["classification"], PERSISTENT)
        self.assertEqual(result["state"], "open")
        self.assertEqual(result["latest_judgment"]["longest_deviation_run"], 3)
        self.assertEqual(len(result["latest_judgment"]["windows"]), 3)
        for item in result["latest_judgment"]["windows"]:
            self.assertTrue(item["qualified"])
            self.assertTrue(item["deviated"])
            self.assertEqual(item["gates"], [])

    def test_incidental_fluctuation_is_distinguished(self):
        self.add_day(1, ["71.20", "72.20"], ["71.00", "72.00"])   # ~0% deviation
        self.add_day(2, ["71.20", "72.20"], ["78.50", "79.50"])   # ~10% deviation
        self.add_day(3, ["71.20", "72.20"], ["71.00", "72.00"])   # ~0% deviation
        self.make_monitor(persistence=2)
        result = self.evaluate()
        self.assertEqual(result["classification"], INCIDENTAL)
        self.assertEqual(result["state"], "open")
        self.assertEqual(result["latest_judgment"]["longest_deviation_run"], 1)
        # Incidental, open alerts do not block review/publish.
        window = result["latest_judgment"]["windows"][0]["window"]
        with self.store.connect() as db:
            blockers = self.alerts.blockers_for_window(
                db, self.campaign, window["start"], "2026-01-04T00:00:00+00:00")
        self.assertEqual(blockers, [])

    def test_insufficient_evidence_when_qualified_windows_fall_short(self):
        # Only one qualified window, persistence needs two.
        self.add_day(1, ["71.20", "72.20"], ["78.50", "79.50"])
        self.make_monitor()
        result = self.evaluate()
        self.assertEqual(result["classification"], INSUFFICIENT)
        self.assertEqual(result["latest_judgment"]["qualified_count"], 1)
        # Empty windows fail the min_samples gate, so the sequence is not
        # fully qualified; the insufficient classification itself also blocks
        # any confirmation or rejection.
        self.assertFalse(result["all_qualified"])
        empty_window = result["latest_judgment"]["windows"][1]
        self.assertFalse(empty_window["qualified"])
        self.assertIn("insufficient_samples:process_b", empty_window["gates"])
        with self.assertRaises(DomainError) as caught:
            self.alerts.dispose(self.campaign, self.alert_id(), "reviewer_a",
                                "confirm", "Trend confirmed by reviewer",
                                result["judgment_sha"])
        self.assertEqual(caught.exception.code, "alert_not_actionable")

    def test_normal_when_no_window_deviates(self):
        for day in (1, 2, 3):
            self.add_day(day, ["71.20", "72.20"], ["71.00", "72.00"])
        self.make_monitor()
        result = self.evaluate()
        self.assertEqual(result["classification"], NORMAL)
        self.assertEqual(result["state"], "open")

    def test_reevaluation_is_idempotent_on_unchanged_revision(self):
        for day in (1, 2, 3):
            self.add_day(day, ["71.20", "72.20"], ["78.50", "79.50"])
        self.make_monitor()
        first = self.evaluate()
        second = self.evaluate()
        self.assertTrue(second["replayed"])
        self.assertEqual(first["judgment_sha"], second["judgment_sha"])
        self.assertEqual(second["current_version"], 1)
        # No duplicate "anomaly messages": one alert, one judgment.
        self.assertEqual(len(self.alerts.list_judgments(self.campaign, self.alert_id())), 1)

    # --- dispositions -------------------------------------------------------

    def test_confirm_and_reject_flow_with_traceability(self):
        for day in (1, 2, 3):
            self.add_day(day, ["71.20", "72.20"], ["78.50", "79.50"])
        self.make_monitor()
        result = self.evaluate()
        confirmed = self.alerts.dispose(
            self.campaign, self.alert_id(), "reviewer_a", "confirm",
            "Persistent drift confirmed against baseline", result["judgment_sha"])
        self.assertFalse(confirmed["replayed"])
        self.assertEqual(confirmed["state"], "confirmed")
        self.assertTrue(confirmed["publishable"])
        self.assertTrue(confirmed["still_applies"])
        # Replay of the identical disposition.
        replay = self.alerts.dispose(
            self.campaign, self.alert_id(), "reviewer_a", "confirm",
            "Persistent drift confirmed against baseline", result["judgment_sha"])
        self.assertTrue(replay["replayed"])
        # Two people can act independently on the same alert.
        other = self.alerts.dispose(
            self.campaign, self.alert_id(), "reviewer_b", "reject",
            "Reject: planned process change explains drift", result["judgment_sha"])
        self.assertEqual(other["latest_disposition"]["reviewer"], "reviewer_b")
        dispositions = self.alerts.list_dispositions(self.campaign, self.alert_id())
        self.assertEqual([item["reviewer"] for item in dispositions], ["reviewer_a", "reviewer_b"])
        # A conflicting decision by the same reviewer on the same judgment is rejected.
        with self.assertRaises(DomainError) as caught:
            self.alerts.dispose(self.campaign, self.alert_id(), "reviewer_a", "reject",
                                "Reviewer changes their mind here", result["judgment_sha"])
        self.assertEqual(caught.exception.code, "disposition_conflict")

    def test_stale_sha_cannot_back_new_statistics(self):
        for day in (1, 2, 3):
            self.add_day(day, ["71.20", "72.20"], ["78.50", "79.50"])
        self.make_monitor()
        first = self.evaluate()
        stale_sha = first["judgment_sha"]
        # A correction revises the source and therefore the window statistics.
        sample_row = self.store.list_samples(self.campaign)[2]
        replacement = {"sample_id": sample_row["sample_id"], "group_id": sample_row["group_id"],
                       "event_time": sample_row["event_time"], "measurements": {"yield_pct": "85.00"}}
        self.ingest.correct(self.campaign, sample_row["sample_id"], replacement,
                            sample_row["content_sha"], "operator")
        second = self.evaluate()
        self.assertEqual(second["current_version"], 2)
        self.assertNotEqual(stale_sha, second["judgment_sha"])
        with self.assertRaises(DomainError) as caught:
            self.alerts.dispose(self.campaign, self.alert_id(), "reviewer_a", "confirm",
                                "Trying to confirm against the old digest", stale_sha)
        self.assertEqual(caught.exception.code, "version_conflict")

    def test_prior_disposition_is_retained_but_marked_stale_after_rejudgment(self):
        for day in (1, 2, 3):
            self.add_day(day, ["71.20", "72.20"], ["78.50", "79.50"])
        self.make_monitor()
        first = self.evaluate()
        self.alerts.dispose(self.campaign, self.alert_id(), "reviewer_a", "confirm",
                            "Persistent drift confirmed against baseline", first["judgment_sha"])
        sample_row = self.store.list_samples(self.campaign)[2]
        replacement = {"sample_id": sample_row["sample_id"], "group_id": sample_row["group_id"],
                       "event_time": sample_row["event_time"], "measurements": {"yield_pct": "85.00"}}
        self.ingest.correct(self.campaign, sample_row["sample_id"], replacement,
                            sample_row["content_sha"], "operator")
        rejudged = self.evaluate()
        self.assertEqual(rejudged["state"], "stale")
        self.assertFalse(rejudged["still_applies"])
        self.assertFalse(rejudged["publishable"])
        # Original disposition is preserved with its own judgment version.
        prior = rejudged["dispositions"][0]
        self.assertEqual(prior["decision"], "confirm")
        self.assertEqual(prior["judgment_version"], 1)
        self.assertEqual(prior["judgment_sha"], first["judgment_sha"])
        # New disposition against the current judgment supersedes it.
        renewed = self.alerts.dispose(
            self.campaign, self.alert_id(), "reviewer_a", "confirm",
            "Re-confirmed after the measurement correction", rejudged["judgment_sha"])
        self.assertEqual(renewed["state"], "confirmed")
        self.assertTrue(renewed["still_applies"])
        self.assertEqual(len(self.alerts.list_dispositions(self.campaign, self.alert_id())), 2)
        self.assertEqual(len(self.alerts.list_judgments(self.campaign, self.alert_id())), 2)

    # --- suspension ---------------------------------------------------------

    def test_window_disqualification_suspends_alert_and_blocks_disposition(self):
        for day in (1, 2, 3):
            self.add_day(day, ["71.20", "72.20"], ["78.50", "79.50"])
        self.make_monitor()
        confirmed = self.evaluate()
        self.alerts.dispose(self.campaign, self.alert_id(), "reviewer_a", "confirm",
                            "Persistent drift confirmed against baseline",
                            confirmed["judgment_sha"])
        # Void a target sample: window 1 drops below min_samples and the alert suspends.
        target = next(row for row in self.store.list_samples(self.campaign)
                      if row["group_id"] == "process_b" and row["event_time"].startswith("2026-01-01"))
        self.ingest.void(self.campaign, target["sample_id"], target["content_sha"],
                         "Late calibration failure invalidated reading", "operator")
        suspended = self.evaluate()
        self.assertEqual(suspended["current_version"], 2)
        self.assertEqual(suspended["state"], "suspended")
        self.assertFalse(suspended["all_qualified"])
        self.assertFalse(suspended["publishable"])
        failed_window = suspended["latest_judgment"]["windows"][0]
        self.assertFalse(failed_window["qualified"])
        self.assertIn("insufficient_samples:process_b", failed_window["gates"])
        # The prior confirmation must not keep presenting itself as valid.
        self.assertIsNotNone(suspended["latest_disposition"])
        self.assertFalse(suspended["still_applies"])
        with self.assertRaises(DomainError) as caught:
            self.alerts.dispose(self.campaign, self.alert_id(), "reviewer_b", "confirm",
                                "Cannot confirm while evidence is suspended",
                                suspended["judgment_sha"])
        self.assertEqual(caught.exception.code, "alert_suspended")

    def test_data_repair_restores_alert_and_rejudges_with_history(self):
        for day in (1, 2, 3):
            self.add_day(day, ["71.20", "72.20"], ["78.50", "79.50"])
        self.make_monitor()
        first = self.evaluate()
        self.alerts.dispose(self.campaign, self.alert_id(), "reviewer_a", "confirm",
                            "Persistent drift confirmed against baseline", first["judgment_sha"])
        target = next(row for row in self.store.list_samples(self.campaign)
                      if row["group_id"] == "process_b" and row["event_time"].startswith("2026-01-01"))
        self.ingest.void(self.campaign, target["sample_id"], target["content_sha"],
                         "Late calibration failure invalidated reading", "operator")
        suspended = self.evaluate()
        self.assertEqual(suspended["state"], "suspended")
        # Late but valid measurement arrives, restoring the minimum sample count.
        self.ingest.ingest(
            self.campaign, "batch_repair",
            [sample("t1_repair", "process_b", 5, 1, "79.00")], "operator")
        restored = self.evaluate()
        self.assertEqual(restored["current_version"], 3)
        self.assertTrue(restored["all_qualified"])
        self.assertEqual(restored["classification"], PERSISTENT)
        # Old decision retained; applicability is explicitly re-evaluated.
        self.assertEqual(restored["state"], "stale")
        self.assertFalse(restored["still_applies"])
        self.assertEqual(len(restored["dispositions"]), 1)
        renewed = self.alerts.dispose(
            self.campaign, self.alert_id(), "reviewer_a", "confirm",
            "Still deviating after the repair landed", restored["judgment_sha"])
        self.assertEqual(renewed["state"], "confirmed")

    def test_excess_rejections_suspend_window(self):
        # Four accepted + two rejected in window 1 -> bad fraction 2/6 > 0.2 budget.
        rows = [
            sample("c1_0", "control", 1, 1, "71.20"),
            sample("c1_1", "control", 2, 1, "72.20"),
            sample("t1_0", "process_b", 3, 1, "78.50"),
            sample("t1_1", "process_b", 4, 1, "79.50"),
            sample("bad_one", "process_b", 5, 1, "999"),
            sample("bad_two", "process_b", 6, 1, "998"),
        ]
        self.ingest.ingest(self.campaign, "batch_day1", rows, "operator")
        self.add_day(2, ["71.20", "72.20"], ["78.50", "79.50"])
        self.add_day(3, ["71.20", "72.20"], ["78.50", "79.50"])
        self.make_monitor()
        result = self.evaluate()
        self.assertFalse(result["all_qualified"])
        self.assertEqual(result["state"], "suspended")
        gates = result["latest_judgment"]["windows"][0]["gates"]
        self.assertIn("excess_rejection_fraction", gates)

    # --- release / review integration --------------------------------------

    def build_analysis(self, analysis_id="analysis_one"):
        # Analysis windows are capped at the campaign's 24h window_hours.
        return self.analyzer.build(
            self.campaign, analysis_id,
            "2026-01-01T00:00:00+00:00", "2026-01-02T00:00:00+00:00", "analyst")

    def approve(self, analysis):
        for reviewer in ("reviewer_a", "reviewer_b"):
            self.publisher.review(self.campaign, analysis["analysis_id"], reviewer, "approve",
                                  "Measurements checked in ledger", analysis["result_sha"])

    def test_open_persistent_alert_blocks_review_and_publish(self):
        for day in (1, 2, 3):
            self.add_day(day, ["71.20", "72.20"], ["78.50", "79.50"])
        self.make_monitor()
        self.evaluate()
        analysis = self.build_analysis()
        with self.assertRaises(DomainError) as caught:
            self.publisher.review(self.campaign, "analysis_one", "reviewer_a", "approve",
                                  "Measurements checked in ledger", analysis["result_sha"])
        self.assertEqual(caught.exception.code, "alert_gate_blocked")
        # Rejecting the alert removes the block and release can proceed.
        alert = self.alerts.get_alert(self.campaign, self.alert_id())
        self.alerts.dispose(self.campaign, self.alert_id(), "reviewer_a", "reject",
                            "Known planned adjustment, rejecting the alert",
                            alert["judgment_sha"])
        self.approve(analysis)
        release = self.publisher.publish(self.campaign, "analysis_one", "release_one",
                                         "publisher", analysis["result_sha"])
        self.assertEqual(release["release_id"], "release_one")
        evidence = release["alert_evidence"]
        self.assertEqual(evidence[0]["alert_id"], self.alert_id())
        self.assertEqual(evidence[0]["state"], "rejected")

    def test_suspended_alert_blocks_publish_until_rejudged_and_confirmed(self):
        for day in (1, 2, 3):
            self.add_day(day, ["71.20", "72.20"], ["78.50", "79.50"])
        self.make_monitor()
        judged = self.evaluate()
        self.alerts.dispose(self.campaign, self.alert_id(), "reviewer_a", "confirm",
                            "Persistent drift confirmed against baseline", judged["judgment_sha"])
        analysis = self.build_analysis()
        target = next(row for row in self.store.list_samples(self.campaign)
                      if row["group_id"] == "process_b" and row["event_time"].startswith("2026-01-01"))
        self.ingest.void(self.campaign, target["sample_id"], target["content_sha"],
                         "Calibration fault discovered after confirmation", "operator")
        self.evaluate()
        # The old frozen analysis is stale after the void; independently the
        # alert gate reports the suspension as the blocking evidence reason.
        with self.assertRaises(DomainError) as caught:
            self.publisher.review(self.campaign, "analysis_one", "reviewer_a", "approve",
                                  "Measurements checked in ledger", analysis["result_sha"])
        self.assertEqual(caught.exception.code, "stale_analysis")
        with self.store.connect() as db:
            blockers = self.alerts.blockers_for_window(
                db, self.campaign, "2026-01-01T00:00:00+00:00",
                "2026-01-04T00:00:00+00:00")
        self.assertEqual([item["block_reason"] for item in blockers],
                         ["suspended_alert_window"])
        # After the repair, the alert becomes stale (not suspended) and still
        # blocks until the reviewer re-confirms against the new judgment.
        self.ingest.ingest(
            self.campaign, "batch_repair",
            [sample("t1_repair", "process_b", 5, 1, "79.00")], "operator")
        restored = self.evaluate()
        self.assertTrue(restored["all_qualified"])
        with self.store.connect() as db:
            blockers = self.alerts.blockers_for_window(
                db, self.campaign, "2026-01-01T00:00:00+00:00",
                "2026-01-04T00:00:00+00:00")
        self.assertEqual([item["block_reason"] for item in blockers],
                         ["stale_alert_disposition"])
        # Re-confirming against the restored judgment clears the gate, and a
        # freshly built analysis can then be reviewed and released.
        self.alerts.dispose(
            self.campaign, self.alert_id(), "reviewer_a", "confirm",
            "Re-confirmed after the repair restored the window",
            restored["judgment_sha"])
        fresh = self.build_analysis("analysis_fresh")
        self.approve(fresh)
        release = self.publisher.publish(
            self.campaign, "analysis_fresh", "release_fresh",
            "publisher", fresh["result_sha"])
        self.assertEqual(release["release_id"], "release_fresh")

    def test_stale_disposition_blocks_release_gate(self):
        for day in (1, 2, 3):
            self.add_day(day, ["71.20", "72.20"], ["78.50", "79.50"])
        self.make_monitor()
        judged = self.evaluate()
        self.alerts.dispose(self.campaign, self.alert_id(), "reviewer_a", "confirm",
                            "Persistent drift confirmed against baseline", judged["judgment_sha"])
        sample_row = self.store.list_samples(self.campaign)[2]
        replacement = {"sample_id": sample_row["sample_id"], "group_id": sample_row["group_id"],
                       "event_time": sample_row["event_time"], "measurements": {"yield_pct": "85.00"}}
        self.ingest.correct(self.campaign, sample_row["sample_id"], replacement,
                            sample_row["content_sha"], "operator")
        rejudged = self.evaluate()
        self.assertEqual(rejudged["state"], "stale")
        # A new analysis built on the revised statistics still cannot rely on
        # the disposition taken against the older judgment.
        analysis = self.build_analysis("analysis_two")
        with self.assertRaises(DomainError) as caught:
            self.publisher.review(self.campaign, "analysis_two", "reviewer_a", "approve",
                                  "Measurements checked in ledger", analysis["result_sha"])
        self.assertEqual(caught.exception.code, "alert_gate_blocked")
        with self.store.connect() as db:
            blockers = self.alerts.blockers_for_window(
                db, self.campaign, "2026-01-01T00:00:00+00:00",
                "2026-01-04T00:00:00+00:00")
        self.assertEqual([item["block_reason"] for item in blockers],
                         ["stale_alert_disposition"])
        # Re-confirming against the current judgment clears the gate.
        self.alerts.dispose(self.campaign, self.alert_id(), "reviewer_a", "confirm",
                            "Re-confirmed after correction", rejudged["judgment_sha"])
        self.approve(analysis)
        self.publisher.publish(self.campaign, "analysis_two", "release_two",
                               "publisher", analysis["result_sha"])

    def test_alert_evidence_endpoint_traces_windows_and_decisions(self):
        for day in (1, 2, 3):
            self.add_day(day, ["71.20", "72.20"], ["78.50", "79.50"])
        self.make_monitor()
        judged = self.evaluate()
        self.alerts.dispose(self.campaign, self.alert_id(), "reviewer_a", "confirm",
                            "Persistent drift confirmed against baseline", judged["judgment_sha"])
        self.build_analysis()
        with self.store.connect() as db:
            evidence = self.alerts.evidence_for_window(
                db, self.campaign, "2026-01-01T00:00:00+00:00",
                "2026-01-04T00:00:00+00:00")
        self.assertEqual(len(evidence), 1)
        item = evidence[0]
        self.assertEqual(item["state"], "confirmed")
        self.assertEqual(item["classification"], PERSISTENT)
        self.assertEqual(len(item["trigger_windows"]), 3)
        self.assertTrue(all(window["qualified"] for window in item["trigger_windows"]))
        self.assertEqual(item["latest_disposition"]["reviewer"], "reviewer_a")
        # A non-overlapping analysis window must not pull the alert in.
        with self.store.connect() as db:
            outside = self.alerts.evidence_for_window(
                db, self.campaign, "2026-02-01T00:00:00+00:00",
                "2026-02-02T00:00:00+00:00")
        self.assertEqual(outside, [])

    def test_list_alerts_state_filter(self):
        for day in (1, 2, 3):
            self.add_day(day, ["71.20", "72.20"], ["78.50", "79.50"])
        self.make_monitor()
        judged = self.evaluate()
        self.assertEqual(self.alerts.list_alerts(self.campaign)["count"], 1)
        self.assertEqual(self.alerts.list_alerts(self.campaign, state="open")["count"], 1)
        self.assertEqual(self.alerts.list_alerts(self.campaign, state="confirmed")["count"], 0)
        self.alerts.dispose(self.campaign, self.alert_id(), "reviewer_a", "confirm",
                            "Persistent drift confirmed against baseline", judged["judgment_sha"])
        self.assertEqual(self.alerts.list_alerts(self.campaign, state="confirmed")["count"], 1)
        with self.assertRaises(DomainError) as caught:
            self.alerts.list_alerts(self.campaign, state="bogus")
        self.assertEqual(caught.exception.code, "invalid_state")

    def test_evidence_verification_covers_alert_chain(self):
        from atelier.evidence import EvidenceService
        for day in (1, 2, 3):
            self.add_day(day, ["71.20", "72.20"], ["78.50", "79.50"])
        self.make_monitor()
        judged = self.evaluate()
        self.alerts.dispose(self.campaign, self.alert_id(), "reviewer_a", "confirm",
                            "Persistent drift confirmed against baseline", judged["judgment_sha"])
        service = EvidenceService(self.store)
        self.assertTrue(service.verify_campaign(self.campaign)["ok"])
        export = service.export(self.campaign)
        self.assertEqual(len(export["payload"]["alerts"]), 1)
        self.assertEqual(len(export["payload"]["alert_judgments"]), 1)
        self.assertEqual(len(export["payload"]["alert_dispositions"]), 1)
        # Tampering with a judgment digest is detected.
        with self.store.transaction() as db:
            db.execute("UPDATE alert_judgments SET judgment_sha=? WHERE campaign_id=? AND alert_id=?",
                       ("0" * 64, self.campaign, self.alert_id()))
        verification = service.verify_campaign(self.campaign)
        self.assertFalse(verification["ok"])
        self.assertIn("alert_judgment_digest", {item["kind"] for item in verification["issues"]})

    def test_audit_trail_records_alert_lifecycle(self):
        for day in (1, 2, 3):
            self.add_day(day, ["71.20", "72.20"], ["78.50", "79.50"])
        self.make_monitor()
        judged = self.evaluate()
        self.alerts.dispose(self.campaign, self.alert_id(), "reviewer_a", "confirm",
                            "Persistent drift confirmed against baseline", judged["judgment_sha"])
        audits = self.store.list_audits(self.campaign, limit=100)
        kinds = [event["kind"] for event in audits]
        self.assertIn("monitor_created", kinds)
        self.assertIn("alert_judged", kinds)
        self.assertIn("alert_dispositioned", kinds)


if __name__ == "__main__":
    unittest.main()
