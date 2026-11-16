import json
from pathlib import Path
import tempfile
import unittest

from atelier.analysis import Analyzer
from atelier.domain import CampaignSpec, DomainError
from atelier.ingest import Ingestor
from atelier.release import Publisher
from atelier.store import Store


def fixture():
    return json.loads((Path(__file__).parents[1] / "fixtures" / "synthetic_experiment.json").read_text())


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.store = Store(Path(self.folder.name) / "ledger.sqlite3")
        self.source = fixture()
        self.spec = CampaignSpec.from_dict(self.source["campaign"])
        self.store.create_campaign(self.spec, "operator")
        self.ingest = Ingestor(self.store)
        self.analyze = Analyzer(self.store)
        self.publish = Publisher(self.store)
        self.window = self.source["window"]

    def add_batch(self, batch_id="batch_one", samples=None):
        return self.ingest.ingest(self.spec.campaign_id, batch_id,
                                  samples if samples is not None else self.source["batches"][0]["samples"],
                                  "operator")

    def build(self, analysis_id="analysis_one"):
        return self.analyze.build(self.spec.campaign_id, analysis_id,
                                  self.window["start"], self.window["end"], "analyst")

    def approve(self, analysis):
        for reviewer in ("reviewer_a", "reviewer_b"):
            self.publish.review(self.spec.campaign_id, analysis["analysis_id"], reviewer,
                                "approve", "Measurements checked in ledger", analysis["result_sha"])

    def test_complete_release_uses_frozen_revision(self):
        added = self.add_batch()
        self.assertEqual(len(added["accepted"]), 4)
        self.assertEqual(added["rejected"], [])
        analysis = self.build()
        self.assertTrue(analysis["quality"]["passed"])
        self.assertEqual(analysis["quality"]["group_counts"], {"control": 2, "process_b": 2})
        self.assertEqual(analysis["contrasts"]["process_b"]["yield_pct"]["difference"], "7.30")
        self.approve(analysis)
        release = self.publish.publish(self.spec.campaign_id, analysis["analysis_id"],
                                       "release_one", "publisher", analysis["result_sha"])
        self.assertEqual(release["analysis_id"], "analysis_one")
        self.assertEqual(release["approved_by"], ["reviewer_a", "reviewer_b"])
        self.assertEqual(self.store.get_release(self.spec.campaign_id, "release_one")["body"]["release_sha"],
                         release["release_sha"])
        self.assertEqual(len(self.store.list_audits(self.spec.campaign_id)), 6)

    def test_repeated_batch_keeps_revision_stable(self):
        first = self.add_batch()
        revision = self.store.get_campaign(self.spec.campaign_id)["revision"]
        second = self.add_batch()
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual(second["accepted"], first["accepted"])
        self.assertEqual(revision, self.store.get_campaign(self.spec.campaign_id)["revision"])
        self.assertEqual(len(self.store.list_samples(self.spec.campaign_id)), 4)

    def test_batch_identifier_rejects_conflicting_content(self):
        self.add_batch()
        changed = self.source["batches"][0]["samples"][:1]
        with self.assertRaises(DomainError) as caught:
            self.add_batch(samples=changed)
        self.assertEqual(caught.exception.code, "batch_conflict")
        self.assertEqual(len(self.store.list_samples(self.spec.campaign_id)), 4)

    def test_duplicate_sample_in_batch_is_rejected(self):
        sample = self.source["batches"][0]["samples"][0]
        result = self.add_batch(samples=[sample, sample])
        self.assertEqual(result["accepted"], ["sample_a"])
        self.assertEqual(result["rejected"][0]["reason"], "duplicate_in_batch")
        self.assertEqual(len(self.ingest.list_rejections(self.spec.campaign_id, "batch_one")), 1)

    def test_same_sample_across_batches_is_not_double_counted(self):
        original = self.source["batches"][0]["samples"][0]
        self.add_batch(samples=[original])
        result = self.add_batch("batch_two", [original])
        self.assertEqual(result["accepted"], [])
        self.assertEqual(result["rejected"][0]["reason"], "already_ingested")
        self.assertEqual(len(self.store.list_samples(self.spec.campaign_id)), 1)

    def test_conflicting_sample_across_batches_is_rejected(self):
        original = self.source["batches"][0]["samples"][0]
        self.add_batch(samples=[original])
        changed = {**original, "measurements": {"yield_pct": "75.00"}}
        result = self.add_batch("batch_two", [changed])
        self.assertEqual(result["rejected"][0]["reason"], "sample_conflict")
        self.assertEqual(self.store.list_samples(self.spec.campaign_id)[0]["measurements"]["yield_pct"], "71.20")

    def test_rejection_budget_blocks_release(self):
        rows = list(self.source["batches"][0]["samples"])
        rows.append({**rows[0], "sample_id": "invalid_one", "measurements": {"yield_pct": "999"}})
        rows.append({**rows[0], "sample_id": "invalid_two", "measurements": {"yield_pct": "999"}})
        result = self.add_batch(samples=rows)
        self.assertEqual(len(result["rejected"]), 2)
        analysis = self.build()
        self.assertEqual(analysis["quality"]["rejected_count"], 2)
        self.assertIn("excess_rejection_fraction", analysis["quality"]["gates"])
        with self.assertRaises(DomainError) as caught:
            self.publish.review(self.spec.campaign_id, "analysis_one", "reviewer_a", "approve",
                                "This analysis was checked", analysis["result_sha"])
        self.assertEqual(caught.exception.code, "quality_failed")

    def test_insufficient_group_blocks_review(self):
        self.add_batch(samples=self.source["batches"][0]["samples"][:2])
        analysis = self.build()
        self.assertIn("insufficient_samples:process_b", analysis["quality"]["gates"])
        with self.assertRaises(DomainError):
            self.publish.review(self.spec.campaign_id, "analysis_one", "reviewer_a", "approve",
                                "Measurements were checked", analysis["result_sha"])

    def test_correction_invalidates_existing_analysis(self):
        self.add_batch()
        prior = self.build()
        sample = self.store.list_samples(self.spec.campaign_id)[0]
        replacement = {"sample_id": sample["sample_id"], "group_id": sample["group_id"],
                       "event_time": sample["event_time"], "measurements": {"yield_pct": "70.00"}}
        corrected = self.ingest.correct(self.spec.campaign_id, sample["sample_id"], replacement,
                                        sample["content_sha"], "operator")
        self.assertTrue(corrected["changed"])
        self.assertFalse(self.analyze.status(self.spec.campaign_id, "analysis_one")["current"])
        with self.assertRaises(DomainError) as caught:
            self.publish.review(self.spec.campaign_id, "analysis_one", "reviewer_a", "approve",
                                "Measurements were checked", prior["result_sha"])
        self.assertEqual(caught.exception.code, "stale_analysis")
        next_result = self.build("analysis_two")
        self.assertNotEqual(next_result["result_sha"], prior["result_sha"])

    def test_void_invalidates_analysis_but_preserves_release(self):
        self.add_batch()
        prior = self.build()
        self.approve(prior)
        release = self.publish.publish(self.spec.campaign_id, "analysis_one", "release_one",
                                       "publisher", prior["result_sha"])
        sample = self.store.list_samples(self.spec.campaign_id)[0]
        self.ingest.void(self.spec.campaign_id, sample["sample_id"], sample["content_sha"],
                         "Measurement device was recalibrated", "operator")
        self.assertFalse(self.analyze.status(self.spec.campaign_id, "analysis_one")["current"])
        self.assertEqual(self.store.get_release(self.spec.campaign_id, "release_one")["body"]["release_sha"],
                         release["release_sha"])
        self.assertEqual(len(self.store.list_samples(self.spec.campaign_id, status="active")), 3)
        self.assertEqual(len(self.store.list_samples(self.spec.campaign_id, status="void")), 1)

    def test_analysis_idempotence_and_conflict(self):
        self.add_batch()
        first = self.build()
        second = self.build()
        self.assertTrue(second["replayed"])
        self.assertEqual(first["result_sha"], second["result_sha"])
        with self.assertRaises(DomainError) as caught:
            self.analyze.build(self.spec.campaign_id, "analysis_one", "2026-01-01T01:00:00Z",
                               self.window["end"], "analyst")
        self.assertEqual(caught.exception.code, "analysis_conflict")

    def test_single_approval_cannot_publish(self):
        self.add_batch()
        analysis = self.build()
        self.publish.review(self.spec.campaign_id, "analysis_one", "reviewer_a", "approve",
                            "Measurements were checked", analysis["result_sha"])
        with self.assertRaises(DomainError) as caught:
            self.publish.publish(self.spec.campaign_id, "analysis_one", "release_one", "publisher",
                                 analysis["result_sha"])
        self.assertEqual(caught.exception.code, "reviews_missing")

    def test_reviewer_cannot_publish_own_release(self):
        self.add_batch()
        analysis = self.build()
        self.approve(analysis)
        with self.assertRaises(DomainError) as caught:
            self.publish.publish(self.spec.campaign_id, "analysis_one", "release_one", "reviewer_a",
                                 analysis["result_sha"])
        self.assertEqual(caught.exception.code, "self_publish")

    def test_rejection_vetoes_publication(self):
        self.add_batch()
        analysis = self.build()
        self.approve(analysis)
        self.publish.review(self.spec.campaign_id, "analysis_one", "reviewer_c", "reject",
                            "Calibration evidence is incomplete", analysis["result_sha"])
        with self.assertRaises(DomainError) as caught:
            self.publish.publish(self.spec.campaign_id, "analysis_one", "release_one", "publisher",
                                 analysis["result_sha"])
        self.assertEqual(caught.exception.code, "review_rejected")

    def test_release_replay_is_stable(self):
        self.add_batch()
        analysis = self.build()
        self.approve(analysis)
        first = self.publish.publish(self.spec.campaign_id, "analysis_one", "release_one", "publisher",
                                     analysis["result_sha"])
        second = self.publish.publish(self.spec.campaign_id, "analysis_one", "release_one", "publisher",
                                      analysis["result_sha"])
        self.assertTrue(second["replayed"])
        self.assertEqual(first["release_sha"], second["release_sha"])
        self.assertEqual(len(self.publish.list_releases(self.spec.campaign_id)), 1)

    def test_spec_version_conflict_and_compatible_extension(self):
        update = dict(self.source["campaign"])
        update["groups"] = ["control", "process_b", "process_c"]
        updated = self.store.update_spec(self.spec.campaign_id, update, 1, "operator")
        self.assertEqual(updated["version"], 2)
        with self.assertRaises(DomainError) as caught:
            self.store.update_spec(self.spec.campaign_id, update, 1, "operator")
        self.assertEqual(caught.exception.code, "version_conflict")
        self.assertEqual(self.store.get_campaign(self.spec.campaign_id)["revision"], 1)

    def test_incompatible_spec_preserves_history(self):
        update = dict(self.source["campaign"])
        update["groups"] = ["control", "another_group"]
        with self.assertRaises(DomainError) as caught:
            self.store.update_spec(self.spec.campaign_id, update, 1, "operator")
        self.assertEqual(caught.exception.code, "incompatible_spec")
        self.assertEqual(self.store.get_campaign(self.spec.campaign_id)["spec"]["version"], 1)

    def test_bad_correction_sha_leaves_source_untouched(self):
        self.add_batch()
        sample = self.store.list_samples(self.spec.campaign_id)[0]
        replacement = {"sample_id": sample["sample_id"], "group_id": sample["group_id"],
                       "event_time": sample["event_time"], "measurements": {"yield_pct": "75.00"}}
        with self.assertRaises(DomainError) as caught:
            self.ingest.correct(self.spec.campaign_id, sample["sample_id"], replacement, "0" * 64, "operator")
        self.assertEqual(caught.exception.code, "version_conflict")
        self.assertEqual(self.store.list_samples(self.spec.campaign_id)[0]["content_sha"], sample["content_sha"])

    def test_half_open_window_excludes_end_boundary(self):
        rows = list(self.source["batches"][0]["samples"])
        rows.append({"sample_id": "on_boundary", "group_id": "control",
                     "event_time": self.window["end"], "measurements": {"yield_pct": "99.00"}})
        self.add_batch(samples=rows)
        analysis = self.build()
        self.assertEqual(analysis["quality"]["accepted_count"], 4)
        self.assertEqual(analysis["groups"]["control"]["yield_pct"]["mean"], "71.70")

    def test_audit_pagination_tracks_mutations(self):
        self.add_batch()
        self.build()
        first = self.store.list_audits(self.spec.campaign_id, limit=2)
        second = self.store.list_audits(self.spec.campaign_id, since_id=first[-1]["id"])
        self.assertEqual(len(first), 2)
        self.assertEqual(len(second), 1)
        self.assertEqual(second[0]["kind"], "analysis_created")
