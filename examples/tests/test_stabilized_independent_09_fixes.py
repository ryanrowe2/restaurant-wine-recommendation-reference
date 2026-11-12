"""Independent reproductions for the nine stabilized-candidate findings.

Uses only synthetic files, synthetic wines and mocked outbound providers. No
production secrets, real customer information or network connections.
"""
from __future__ import annotations
from claim_fixture import claim_nonce
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'examples'))
sys.path.insert(0,str(ROOT/'scripts'))
import acceptance_gate
from dbutil import SQLiteDatabase
from catalog_store import CatalogStore
from delivery import EventStore, Preferences, fingerprint_for, process_event, SubmissionUncertain
from pipeline import (ContractError, Wine, select_candidates,
                      validate_selection_priority, create_deterministic_recommendations)
from providers import PostmarkReceiptVerifier, StructuredRecommender


class Response:
    status=200
    def __init__(self,doc):self.doc=doc
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def read(self,limit):return json.dumps(self.doc).encode()


class IndependentNineFindingTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.work=Path(self.temp.name)

    def test_permission_fields_cannot_grant_publication(self):
        # This reproduces the original forged JSON status weakness without
        # attempting real authorization or publishing to GitHub.
        fake=self.work/'minimal.zip'
        with zipfile.ZipFile(fake,'w') as z:z.writestr('restaurant-wine-recommendation-reference/README.md','safe')
        fields={'portfolio_permission_pending':[], 'deployment_checks_pending':[],
                'post_publication_checks_pending':[], 'external_not_verified':[],
                'registered_findings':20,'critical_high_open':[], 'test_count':190}
        with patch.object(acceptance_gate,'scan_distribution',return_value=[]), \
             patch.object(acceptance_gate,'_checks',return_value=([{'check':'mock','passed':True}],fields)):
            report=acceptance_gate.verify(fake,expected_sha256=hashlib.sha256(fake.read_bytes()).hexdigest())
        self.assertTrue(report['offline_gate_passed'])
        self.assertFalse(report['portfolio_publication_ready'])
        self.assertFalse(report['publication_ready'])
        self.assertIn('APPROVAL PENDING',report['decision'])

    def test_issue_removal_breaks_baseline(self):
        original=json.loads((ROOT/'quality/issue_register.json').read_text())
        baseline=(ROOT/'quality/issue_history_baseline.json').read_bytes()
        self.assertEqual([],acceptance_gate.check_historical_issue_inventory(original,baseline))
        original['findings']=original['findings'][:1]
        failures=acceptance_gate.check_historical_issue_inventory(original,baseline)
        self.assertGreaterEqual(len(failures),10)
        self.assertTrue(any('V4-' in item for item in failures))
        tampered=baseline.replace(b'V4-01',b'V4-99')
        self.assertTrue(acceptance_gate.check_historical_issue_inventory(original,tampered))

    def test_unexecuted_test_cannot_close_issue(self):
        cases=[{'id':'fake','regression':'test_not_discovered'}]
        self.assertEqual(1,len(acceptance_gate.regression_evidence_errors(cases,['test_cases.Accepted.test_real_pass'])))
        self.assertEqual([],acceptance_gate.regression_evidence_errors(cases,['test_cases.Accepted.test_not_discovered']))
        expected={'fake':'test_cases.Accepted.test_not_discovered'}
        self.assertTrue(acceptance_gate.regression_evidence_errors(cases,['test_other.Forged.test_not_discovered'],expected))
        self.assertEqual([],acceptance_gate.regression_evidence_errors(cases,['test_cases.Accepted.test_not_discovered'],expected))

    def test_wrong_release_digest_is_rejected(self):
        archive=self.work/'release.zip'
        with zipfile.ZipFile(archive,'w') as z:z.writestr('restaurant-wine-recommendation-reference/README.md','safe')
        with patch.object(acceptance_gate,'scan_distribution',side_effect=AssertionError('must reject before scanning')):
            mismatch=acceptance_gate.verify(archive,expected_sha256='0'*64)
        self.assertFalse(mismatch['offline_gate_passed'])
        self.assertFalse(mismatch['artifact_identity_verified'])
        self.assertIn('digest mismatch',mismatch['decision'])

    @unittest.skipUnless(os.name=='posix','POSIX path/permissions regression')
    def test_sqlite_rejects_symlink_swap(self):
        primary=self.work/'first.sqlite'
        secondary=self.work/'second.sqlite'
        db=SQLiteDatabase(primary)
        with db.connect() as conn:conn.execute('CREATE TABLE expected(value TEXT)')
        other=SQLiteDatabase(secondary)
        with other.connect() as conn:
            conn.execute('CREATE TABLE expected(value TEXT)')
            conn.execute("INSERT INTO expected VALUES ('wrong_database')")
        primary.rename(self.work/'previous.sqlite')
        primary.symlink_to(secondary)
        with self.assertRaisesRegex(ValueError,'symlink'):
            with db.connect() as conn:conn.execute('SELECT * FROM expected')
        other.close();db.close()

    def test_sqlite_relative_path_survives_chdir(self):
        a=self.work/'a';b=self.work/'b';a.mkdir();b.mkdir()
        start=Path.cwd()
        try:
            os.chdir(a)
            db=SQLiteDatabase('relative.sqlite')
            with db.connect() as conn:conn.execute('CREATE TABLE original(value TEXT)')
            os.chdir(b)
            with db.connect() as conn:
                self.assertEqual(('original',),conn.execute("SELECT name FROM sqlite_master WHERE name='original'").fetchone())
            self.assertFalse((b/'relative.sqlite').exists())
        finally:
            os.chdir(start)
            db.close()

    def test_provider_late_receipt_is_fenced(self):
        store=EventStore(self.work/'events.sqlite')
        prefs=Preferences(category='red',sweetness='dry')
        recipient='example@example.test';token='late-ack'
        store.enqueue_request(token=token,venue='venue_a',recipient=recipient,preferences=prefs,now=1)
        fp=fingerprint_for('venue_a',recipient,prefs)
        first=store.claim(token,fp,now=1)
        store.mark_dispatch_started(token,first.attempt,now=2,nonce=claim_nonce(store,token,first.attempt))
        store.heartbeat(token,first.attempt,now=20,lease_seconds=30,nonce=claim_nonce(store,token,first.attempt))
        self.assertEqual(0,store.reap_expired_submissions(now=31))
        self.assertEqual(1,store.reap_expired_submissions(now=51))
        store.mark_provider_accepted(token,first.attempt,'receipt-id',now=52,nonce=first.nonce)
        self.assertEqual('provider_accepted',store.summary(token)[0])
        store.close()

    def test_stale_provider_receipt_cannot_confirm_another_attempt(self):
        store=EventStore(self.work/'events2.sqlite')
        prefs=Preferences(category='red',sweetness='dry'); token='stale-ack'
        store.enqueue_request(token=token,venue='venue_a',recipient='p@example.test',preferences=prefs,now=1)
        fp=fingerprint_for('venue_a','p@example.test',prefs)
        old=store.claim(token,fp,now=1)
        store.mark_dispatch_started(token,old.attempt,now=2,nonce=claim_nonce(store,token,old.attempt))
        store.reap_expired_submissions(now=40)
        with self.assertRaisesRegex(ValueError,'provider submission may have been accepted'):
            store.reconcile(token,now=41,decision='confirmed_not_sent',evidence='Verified no submission at provider dashboard')
        store.reconcile(token,now=42,decision='cancel',evidence='Review closed potential duplicate risk')
        with self.assertRaises(RuntimeError):
            store.mark_provider_accepted(token,old.attempt,'stale-provider-reference',now=44,nonce=old.nonce)
        self.assertEqual('reconciled_cancelled',store.summary(token)[0])
        store.close()

    def test_submitting_heartbeat_can_extend_remote_send(self):
        store=EventStore(self.work/'heartbeat.sqlite')
        prefs=Preferences(category='red',sweetness='dry')
        store.enqueue_request(token='pending-send',venue='venue_a',recipient='a@example.com',preferences=prefs,now=1)
        claim=store.claim('pending-send',fingerprint_for('venue_a','a@example.com',prefs),now=1)
        store.mark_dispatch_started('pending-send',claim.attempt,now=2,nonce=claim_nonce(store,'pending-send',claim.attempt))
        # The original code rejected heartbeat once the worker began an HTTP submission.
        store.heartbeat('pending-send',claim.attempt,now=20,lease_seconds=30,nonce=claim_nonce(store,'pending-send',claim.attempt))
        self.assertEqual(0,store.reap_expired_submissions(now=31))
        store.close()

    def test_provider_timeout_after_reaper_keeps_reconciliation_hold(self):
        event=EventStore(self.work/'reaped.sqlite')
        catalog=CatalogStore(self.work/'reaped-catalog.sqlite')
        wines=[Wine('venue_a',str(i),'Wine '+str(i),'p','France','red','merlot',
                    'dry',frozenset(),True,True,frozenset()) for i in range(3)]
        catalog.replace_venue('venue_a',wines)
        def delayed_provider(recs):
            self.assertEqual(1,event.reap_expired_submissions(now=34))
            raise SubmissionUncertain('local simulated timeout')
        self.assertEqual('reconciliation_required',process_event(event,token='held',
            claimed_venue='venue_a',authenticated_venue='venue_a',recipient_key='p@example.test',
            preferences=Preferences('red','dry'),sender=delayed_provider,now=1,
            catalog_store=catalog,clock=lambda:1))
        self.assertEqual('reconciliation_required',event.summary('held')[0])
        event.close();catalog.close()

    def test_model_cannot_skip_stricter_tier(self):
        def wine(ident,sweetness):
            return Wine('v',ident,'Wine '+ident,'pron','France','red','merlot',sweetness,
                        frozenset(),True,True,frozenset())
        catalog=[wine('dry1','dry')]+[wine('semi'+str(i),'semi-dry') for i in range(6)]
        prefs=Preferences(category='red',sweetness='dry')
        selected=select_candidates(catalog,'v',prefs,max_candidates=12)
        self.assertEqual(7,len(selected.wines))
        model_only_semi=type(selected)(tuple(w for w in selected.wines if w.sweetness=='semi-dry')[:3],selected.stages_visited)
        output=create_deterministic_recommendations(model_only_semi,prefs)
        with self.assertRaisesRegex(ContractError,'strictly preferred'):
            validate_selection_priority(output,selected)
        good=create_deterministic_recommendations(selected,prefs)
        validate_selection_priority(good,selected)

    def test_postmark_accepts_display_sender(self):
        ref='example-provider-id';event_hash='a'*64
        receipt={'MessageID':ref,'Status':'Sent','From':'"Demo Sender" <sender@example.com>',
                 'Recipients':['p@example.com'],'Metadata':{'PalateEvent':event_hash}}
        verifier=PostmarkReceiptVerifier('synthetic-server-token','sender@example.com',opener=lambda req,timeout:Response(receipt))
        self.assertTrue(verifier(ref,'p@example.com',event_hash))
        receipt['From']='"Another Person" <wrong@example.com>'
        self.assertFalse(verifier(ref,'p@example.com',event_hash))

if __name__=='__main__':unittest.main()
