"""Independent regression probes for the 18 November 11 v2 audit findings."""
from __future__ import annotations
from claim_fixture import claim_nonce
import base64
import hashlib
import hmac
import io
import json
import os
import stat
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from delivery import EventStore,process_event,fingerprint_for,MAX_QUEUED_EVENTS
from pipeline import Preferences,ContractError,load_catalog,create_deterministic_recommendations,select_candidates
from catalog_store import CatalogStore
from webhook import FormBinding,verify_and_parse
from webapp import WebhookApp
from providers import PostmarkSender,StructuredRecommender,MODEL_OUTPUT_SCHEMA
from postgres_catalog import legacy_row_to_wine,extract_reviewable_pairings,PostgresCatalogStore
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts'))
import release_gate

class ResolutionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store=EventStore(Path(self.temp.name)/'events.db')
        self.addCleanup(self.store.close)
        self.pref=Preferences(category='red',sweetness='dry')
    def enqueue(self,token='t',now=1):
        return self.store.enqueue_request(token=token,venue='venue_a',recipient='r@example.test',preferences=self.pref,now=now)
    def make_uncertain(self,token='t'):
        self.enqueue(token)
        fp=fingerprint_for('venue_a','r@example.test',self.pref)
        self.store.claim(token,fp,now=1)
        self.store.mark_dispatch_started(token,1,now=2,nonce=claim_nonce(self.store,token,1))
        self.store.mark_uncertain(token,1,now=3,nonce=claim_nonce(self.store,token,1))
    def test_01_cannot_requeue_scrubbed_event(self):
        self.make_uncertain()
        self.assertEqual(self.store.scrub_aged_unresolved(now=90*86400+20,age_seconds=90*86400),1)
        with self.assertRaisesRegex(ValueError,'provider submission may have been accepted'):
            self.store.reconcile('t',now=90*86400+30,decision='confirmed_not_sent',evidence='No provider acceptance found in lookup')
        self.assertEqual(self.store.summary('t')[0],'reconciliation_required')
    def test_02_cannot_requeue_poisoned_event(self):
        self.enqueue()
        self.store.poison('t',now=4)
        with self.assertRaisesRegex(ValueError,'payload unavailable'):
            self.store.reconcile('t',now=5,decision='confirmed_not_sent',evidence='Investigated poison request in provider logs')
        self.assertEqual(self.store.summary('t')[0],'permanent_failure')
    def test_03_completed_does_not_block_active_capacity(self):
        with patch('delivery.MAX_QUEUED_EVENTS',1):
            self.enqueue()
            fp=fingerprint_for('venue_a','r@example.test',self.pref)
            self.store.claim('t',fp,now=1)
            self.store.mark_permanent_failure('t',1,now=2,nonce=claim_nonce(self.store,'t',1))
            self.assertEqual(self.store.enqueue_request(token='t2',venue='venue_a',recipient='r@example.test',preferences=self.pref,now=3),'queued')
            self.store.worker_heartbeat(now=3)
            self.assertEqual(self.store.readiness(now=3)['queued_count'],1)
    @unittest.skipUnless(os.name=='posix','POSIX permissions only')
    def test_04_sqlite_file_mode_private_under_world_umask(self):
        old=os.umask(0o022)
        try:
            p=Path(self.temp.name)/'new-private'/ 'state.sqlite3'
            store=EventStore(p);store.close()
            self.assertEqual(stat.S_IMODE(p.stat().st_mode),0o600)
            self.assertEqual(stat.S_IMODE(p.parent.stat().st_mode),0o700)
        finally:os.umask(old)
    def test_05_inferred_pairings_not_used_before_review(self):
        row=(1,'Example',True,False,'Fiction','red','merlot','notes','full','dry','Not suitable with steak or red meat',None)
        wine=legacy_row_to_wine(row)
        self.assertEqual(wine.pairings,frozenset())
    def test_07_deep_signed_input_returns_controlled_error(self):
        binding=FormBinding('F','venue_a',b'long-synthetic-secret')
        raw=b'['*12000+b'0'+b']'*12000
        sig='sha256='+base64.b64encode(hmac.new(binding.secret,raw,hashlib.sha256).digest()).decode()
        from pipeline import InputError
        with self.assertRaises(InputError):verify_and_parse(raw,sig,binding)
    def test_08_poisoned_nested_record_does_not_starve_queue(self):
        self.enqueue('bad');self.enqueue('good')
        deeply_nested='['*12000+'0'+']'*12000
        with self.store._connect() as conn:
            conn.execute('UPDATE events SET request_json=? WHERE token=?',(deeply_nested,'bad'))
        due=self.store.due_requests(now=3)
        self.assertEqual([token for token,_ in due],['good'])
        self.assertEqual(self.store.summary('bad')[0],'permanent_failure')
    def test_09_secret_pattern_with_test_substring_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            (Path(td)/'credential.py').write_text('password='+'"'+'PRODpassw0rdtestqwertyXYZ012345'+'"')
            self.assertTrue(release_gate.scan(td))
    def test_10_readiness_detects_absent_catalog(self):
        catalog=CatalogStore(Path(self.temp.name)/'empty.db')
        self.addCleanup(catalog.close)
        binding=FormBinding('F','venue_a',b'long-synthetic-secret')
        app=WebhookApp(self.store,{'a':binding},catalog=catalog,clock=lambda:100)
        self.store.worker_heartbeat(now=100)
        response=[]
        raw=b''.join(app({'REQUEST_METHOD':'GET','PATH_INFO':'/health'},lambda status,headers:response.append(status)))
        self.assertTrue(response[0].startswith('503'))
        self.assertFalse(json.loads(raw)['catalog_ready'])
    def test_10_postgres_catalog_error_results_in_503(self):
        class BrokenCatalog:
            def has_venue(self,venue):raise Exception('synthetic PostgreSQL connection failure')
        binding=FormBinding('F','venue_a',b'long-synthetic-secret')
        self.store.worker_heartbeat(now=100)
        app=WebhookApp(self.store,{'a':binding},catalog=BrokenCatalog(),clock=lambda:100)
        response=[]
        raw=b''.join(app({'REQUEST_METHOD':'GET','PATH_INFO':'/health'},lambda status,headers:response.append(status)))
        self.assertTrue(response[0].startswith('503'))
        self.assertFalse(json.loads(raw)['ready'])
    def test_11_retention_negative_age_refused(self):
        self.make_uncertain()
        with self.assertRaises(ValueError):self.store.scrub_aged_unresolved(now=5,age_seconds=-1)
        with self.store._connect() as conn:self.assertIsNotNone(conn.execute('SELECT request_json FROM events').fetchone()[0])
    def test_12_blank_venue_binding_refused(self):
        with self.assertRaises(ValueError):FormBinding('F','   ',b'long-synthetic-secret')
    def test_13_postmark_explicit_reject(self):
        from delivery import PermanentProviderReject
        class Reply:
            status=200
            def __enter__(self):return self
            def __exit__(self,*args):return False
            def read(self,n):return b'{"ErrorCode":406,"Message":"Recipient suppressed"}'
        sender=PostmarkSender('fake','sender@example.test',opener=lambda req,timeout:Reply())
        with self.assertRaises(PermanentProviderReject):sender.send('r@example.test',[])
    def test_14_model_outage_falls_back_deterministically(self):
        catalog=CatalogStore(Path(self.temp.name)/'catalog.db')
        self.addCleanup(catalog.close)
        catalog.replace_venue('venue_a',[w for w in load_catalog() if w.restaurant_id=='venue_a'])
        outputs=[]
        def failed_model(s,p):raise ContractError('Model unavailable')
        status=process_event(self.store,token='t',claimed_venue='venue_a',authenticated_venue='venue_a',recipient_key='r@example.test',preferences=self.pref,catalog_store=catalog,recommender=failed_model,sender=lambda recs:(outputs.extend(recs) or 'accepted'),now=1)
        self.assertEqual(status,'provider_accepted')
        self.assertEqual(len(outputs),3)
    def test_16_document_no_longer_contradicts_notice(self):
        doc=(Path(__file__).resolve().parents[2]/'docs/prepublication-repair-matrix.md').read_text()
        self.assertNotIn('no automated diner fallback email',doc)
    def test_17_archive_gate_checks_top_level_files(self):
        with tempfile.TemporaryDirectory() as td:
            a=Path(td)/'bad.zip'
            with zipfile.ZipFile(a,'w') as z:
                z.writestr('restaurant-wine-recommendation-reference/README.md','Safe')
                z.writestr('outside.txt','password='+'"'+'PRODpassw0rdtestqwertyXYZ012345'+'"')
            self.assertTrue(any('archive path' in x for x in release_gate.scan_distribution(a)))
    def test_18_provider_requests_only_ids(self):
        self.assertEqual(MODEL_OUTPUT_SCHEMA['properties']['recommendations']['items']['required'],['wine_id'])
        selection=select_candidates(load_catalog(),'venue_a',self.pref)
        chosen=[{'wine_id':w.wine_id} for w in selection.wines[:3]]
        class Response:
            status=200
            def __enter__(self):return self
            def __exit__(self,*args):return False
            def read(self,n):return json.dumps({'choices':[{'message':{'content':json.dumps({'recommendations':chosen})}}]}).encode()
        model=StructuredRecommender('fake',opener=lambda req,timeout:Response())
        self.assertEqual(len(model(selection,self.pref)),3)

if __name__=='__main__':unittest.main()
