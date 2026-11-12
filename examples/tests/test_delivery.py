"""Persistent retry, collision and trust-context tests (offline only)."""
from claim_fixture import claim_nonce


import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from delivery import EventStore, TokenCollision, process_event, KnownNotAccepted, fingerprint_for  # noqa: E402
from pipeline import CatalogDataError, ContractError, Preferences  # noqa: E402


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "delivery.sqlite3"
        self.store = EventStore(self.path)
        self.preferences = Preferences(category="red", sweetness="dry", serving_format="glass")
        self.sent = []

    def send(self, recs):
        self.sent.append(recs)
        return "synthetic-delivery-ref"

    def process(self, *, now=100.0, sender=None, venue="venue_a", claimed="venue_a", token="token1", recipient="synthetic-recipient@example.test"):
        return process_event(
            self.store,
            token=token,
            claimed_venue=claimed,
            authenticated_venue=venue,
            recipient_key=recipient,
            preferences=self.preferences,
            sender=sender or self.send,
            now=now,
        )

    def test_success_and_process_restart_are_idempotent(self):
        self.assertEqual(self.process(), "provider_accepted")
        self.store = EventStore(self.path)
        self.assertEqual(self.process(now=105), "already_accepted")
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.store.summary("token1"), ("provider_accepted", 1, "synthetic-delivery-ref"))

    def test_transport_failure_can_be_retried(self):
        def fail(_):
            raise KnownNotAccepted("verified no-acceptance")

        self.assertEqual(self.process(sender=fail), "retryable_failure")
        self.assertEqual(self.store.summary("token1")[0], "retryable_failure")
        self.assertEqual(self.process(now=101), "retry_scheduled")
        self.assertEqual(self.process(now=131), "provider_accepted")
        self.assertEqual(self.store.summary("token1")[1], 2)
        self.assertEqual(len(self.sent), 1)

    def test_exponential_retry_backoff_and_exhaustion(self):
        def fail(_):
            raise KnownNotAccepted("verified no-acceptance")

        self.assertEqual(self.process(sender=fail, now=100), "retryable_failure")
        self.assertEqual(self.process(sender=fail, now=129), "retry_scheduled")
        self.assertEqual(self.process(sender=fail, now=130), "retryable_failure")
        self.assertEqual(self.process(sender=fail, now=189), "retry_scheduled")
        self.assertEqual(self.process(sender=fail, now=190), "retry_exhausted")
        self.assertEqual(self.process(sender=fail, now=900), "retry_exhausted")
        self.assertEqual(self.store.summary("token1"), ("retry_exhausted", 3, None))

    def _admitted_crash_fingerprint(self):
        self.store.enqueue_request(token='crash-token',venue='venue_a',
            recipient='synthetic-recipient@example.test',preferences=self.preferences,now=99)
        return fingerprint_for('venue_a','synthetic-recipient@example.test',self.preferences)

    def test_unexpired_claim_blocks_duplicate_and_expired_claim_can_retry(self):
        # Model a crash that occurred after claim, before any provider call.
        fingerprint = self._admitted_crash_fingerprint()
        self.assertEqual(self.store.claim("crash-token", fingerprint, now=100, lease_seconds=30).status, "claimed")
        self.assertEqual(self.store.claim("crash-token", fingerprint, now=120).status, "in_flight")
        self.assertEqual(self.store.claim("crash-token", fingerprint, now=131).status, "claimed")
        self.assertEqual(self.store.summary("crash-token")[1], 2)

    def test_stale_worker_cannot_overwrite_new_claim(self):
        fp=self._admitted_crash_fingerprint()
        first = self.store.claim("crash-token", fp, now=100, lease_seconds=30)
        second = self.store.claim("crash-token", fp, now=131, lease_seconds=30)
        with self.assertRaises(RuntimeError):
            self.store.mark_provider_accepted("crash-token", first.attempt, "old-ref", now=132,nonce=claim_nonce(self.store,"crash-token",first.attempt))
        self.store.mark_dispatch_started("crash-token", second.attempt, now=132,nonce=claim_nonce(self.store,"crash-token",second.attempt))
        self.store.mark_provider_accepted("crash-token", second.attempt, "new-ref", now=133,nonce=claim_nonce(self.store,"crash-token",second.attempt))
        self.assertEqual(self.store.summary("crash-token"), ("provider_accepted", 2, "new-ref"))

    def test_crashed_workers_cannot_bypass_retry_limit(self):
        fp=self._admitted_crash_fingerprint()
        first = self.store.claim("crash-token", fp, now=100, lease_seconds=5)
        second = self.store.claim("crash-token", fp, now=106, lease_seconds=5)
        third = self.store.claim("crash-token", fp, now=112, lease_seconds=5)
        self.assertEqual((first.attempt, second.attempt, third.attempt), (1, 2, 3))
        self.assertEqual(self.store.claim("crash-token", fp, now=114).status, "in_flight")
        self.assertEqual(self.store.claim("crash-token", fp, now=118).status, "retry_exhausted")
        self.assertEqual(self.store.summary("crash-token"), ("retry_exhausted", 3, None))

    def test_rejects_claimed_venue_spoofing_before_storing(self):
        with self.assertRaises(PermissionError):
            self.process(claimed="venue_b")
        self.assertIsNone(self.store.summary("token1"))

    def test_same_token_cannot_be_reused_for_different_request(self):
        self.assertEqual(self.process(), "provider_accepted")
        self.preferences = Preferences(category="white", sweetness="dry", serving_format="glass")
        with self.assertRaises(TokenCollision):
            self.process(now=110)

    def test_same_token_cannot_be_reused_for_another_recipient(self):
        self.assertEqual(self.process(), "provider_accepted")
        with self.assertRaises(TokenCollision):
            self.process(now=111, recipient="another-synthetic-recipient@example.test")

    def test_insufficient_inventory_is_terminal_not_retried(self):
        self.assertEqual(self.process(venue="venue_sparse", claimed="venue_sparse"), "permanent_failure")
        self.assertEqual(self.process(venue="venue_sparse", claimed="venue_sparse", now=105), "permanent_failure")
        self.assertEqual(self.store.summary("token1"), ("permanent_failure", 1, None))
        self.assertEqual(len(self.sent), 0)

    def test_corrupt_catalog_is_retryable_not_a_diner_failure(self):
        with patch("delivery.load_catalog", side_effect=CatalogDataError("bad synthetic row")):
            self.assertEqual(self.process(), "retryable_failure")
        self.assertEqual(self.process(now=101), "retry_scheduled")
        self.assertEqual(self.process(now=131), "provider_accepted")
        self.assertEqual(self.store.summary("token1")[1], 2)

    def test_malformed_generated_response_is_an_operational_failure(self):
        with patch("delivery.validate_recommendation_contract", side_effect=ContractError("bad schema")):
            self.assertEqual(self.process(), "retryable_failure")
        self.assertEqual(self.process(now=131), "provider_accepted")


if __name__ == "__main__":
    unittest.main()
