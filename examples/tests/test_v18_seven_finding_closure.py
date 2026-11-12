"""Independent synthetic regression cases for all seven v17 findings.

No network requests, credentials, customer data, or publication side effects.
"""
from __future__ import annotations
import base64
import hashlib
import hmac
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
import zipfile
from contextlib import closing

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'examples'),str(ROOT/'scripts')]
from release_gate import _safe_svg, _decoded_svg_credential, scan_distribution
from catalog_store import CatalogStore
from pipeline import CatalogDataError, Preferences, InputError, load_catalog, create_deterministic_recommendations
from delivery import EventStore, process_event, PermanentProviderReject, SubmissionUncertain, fingerprint_for
from runtime import run_due
from webhook import FormBinding, verify_and_parse
from webapp import WebhookApp

class V18SevenFindingClosure(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory(prefix='palate-v18-')
        self.addCleanup(tmp.cleanup)
        self.tmp=Path(tmp.name)
        self.prefs=Preferences(category='red',sweetness='dry')

    def event(self,token='synthetic',now=1.):
        store=EventStore(self.tmp/(token+'.sqlite'))
        self.addCleanup(store.close)
        store.enqueue_request(token=token,venue='venue_a',recipient='guest@example.test',preferences=self.prefs,now=now)
        return store

    def test_a17_01_split_svg_and_deep_entity_credentials_blocked(self):
        token='sk-proj-'+'Z'*38
        split=f'<svg xmlns="http://www.w3.org/2000/svg"><text><tspan>sk-proj-</tspan><tspan>{"Z"*38}</tspan></text></svg>'
        nested=token
        for _ in range(8):
            nested=nested.replace('&','&amp;') if '&' in nested else '&#115;'+nested[1:]
        # Nested XML entity escaping of first characters through multiple levels.
        encoded='&#115;k-proj-'+'Z'*38
        for _ in range(8): encoded=encoded.replace('&','&amp;')
        for payload in (split,f'<svg xmlns="http://www.w3.org/2000/svg"><desc>{encoded}</desc></svg>'):
            with self.subTest(kind='split' if payload==split else 'nested'):
                self.assertTrue(_safe_svg(payload))
                self.assertTrue(_decoded_svg_credential(payload))
        self.assertFalse(_decoded_svg_credential('<svg xmlns="http://www.w3.org/2000/svg"><title>Safe example</title></svg>'))

    def test_a17_02_nonempty_zip_directories_rejected(self):
        for compression in (zipfile.ZIP_STORED,zipfile.ZIP_DEFLATED):
            archive=self.tmp/('bad%d.zip'%compression)
            with zipfile.ZipFile(archive,'w') as z:
                z.writestr('restaurant-wine-recommendation-reference/',b'SYNTHETIC-UNINSPECTED',compress_type=compression)
                z.writestr('restaurant-wine-recommendation-reference/README.md','Safe')
            issues=scan_distribution(archive)
            self.assertTrue(any('directory' in issue.lower() for issue in issues),issues)

    def test_a17_03_unknown_catalog_trigger_rejected_before_writes(self):
        database=self.tmp/'catalog.sqlite'
        store=CatalogStore(database);store.close()
        with closing(sqlite3.connect(database)) as con,con:
            con.execute("CREATE TRIGGER unauthorized AFTER INSERT ON catalog BEGIN UPDATE catalog SET wine_name='rewritten' WHERE wine_id=NEW.wine_id; END")
        with self.assertRaisesRegex(ValueError,'unexpected SQLite catalog trigger'):
            CatalogStore(database)

    def test_a17_03_mid_session_trigger_rewrite_causes_atomic_rollback(self):
        store=CatalogStore(self.tmp/'catalog2.sqlite');self.addCleanup(store.close)
        wines=[w for w in load_catalog() if w.restaurant_id=='venue_a']
        store.replace_venue('venue_a',wines)
        baseline=store.names_for_venue('venue_a')
        with store._connect() as con:
            con.execute("CREATE TRIGGER unauthorized AFTER INSERT ON catalog BEGIN UPDATE catalog SET wine_name='rewritten' WHERE wine_id=NEW.wine_id; END")
        with self.assertRaisesRegex(CatalogDataError,'differs from reviewed inventory'):
            store.replace_venue('venue_a',wines)
        self.assertEqual(store.names_for_venue('venue_a'),baseline)

    def test_a17_04_admission_ledger_foreign_key_drift_rejected(self):
        database=self.tmp/'event.sqlite'
        event=EventStore(database);event.close()
        with closing(sqlite3.connect(database)) as con,con:
            con.execute('DROP TABLE admission_ledger')
            con.execute('''CREATE TABLE admission_ledger (
              token TEXT PRIMARY KEY,venue TEXT NOT NULL,recipient_hash TEXT NOT NULL,admitted_at REAL NOT NULL)''')
            con.execute("INSERT INTO admission_ledger VALUES ('orphan','venue_a','hash',1)")
        with self.assertRaisesRegex(ValueError,'schema drift'):
            EventStore(database)

    def test_a17_04_unreviewed_event_trigger_rejected(self):
        database=self.tmp/'event_trigger.sqlite'
        event=EventStore(database);event.close()
        with closing(sqlite3.connect(database)) as con,con:
            con.execute("CREATE TRIGGER forged AFTER INSERT ON admission_ledger BEGIN DELETE FROM admission_ledger WHERE token=NEW.token; END")
        with self.assertRaisesRegex(ValueError,'unexpected SQLite delivery'):
            EventStore(database)

    def test_a17_05_expired_malformed_claim_quarantines_without_crashing_scheduler(self):
        event=self.event(token='bad',now=1)
        fp=fingerprint_for('venue_a','guest@example.test',self.prefs)
        claim=event.claim('bad',fp,now=2,lease_seconds=5)
        with event._connect() as con:con.execute("UPDATE events SET request_json='{}' WHERE token='bad'")
        catalog=CatalogStore(self.tmp/'catalog.sqlite');self.addCleanup(catalog.close)
        catalog.replace_venue('venue_a',[w for w in load_catalog() if w.restaurant_id=='venue_a'])
        event.enqueue_request(token='good',venue='venue_a',recipient='different@example.test',preferences=self.prefs,now=3)
        sender=type('SyntheticSender',(),{'send':lambda self,target,recs,token_hash=None:'accepted-reference'})()
        result=run_due(event,catalog,create_deterministic_recommendations,sender,now=lambda:40.)
        self.assertEqual(event.summary('bad')[0],'permanent_failure')
        self.assertEqual(result['processed']['good'],'provider_accepted')
        with self.assertRaises(RuntimeError):event.mark_dispatch_started('bad',claim.attempt,now=41,nonce=claim.nonce)

    def test_a17_05_poison_sending_only_after_claim_expiry(self):
        event=self.event(token='poison')
        claim=event.claim('poison',fingerprint_for('venue_a','guest@example.test',self.prefs),now=2,lease_seconds=5)
        with self.assertRaisesRegex(RuntimeError,'Active worker'):
            event.poison('poison',now=3)
        event.poison('poison',now=8)
        self.assertEqual(event.summary('poison')[0],'permanent_failure')
        with self.assertRaises(RuntimeError):event.mark_dispatch_started('poison',claim.attempt,now=9,nonce=claim.nonce)

    def test_a17_06_definitive_provider_nonacceptance_recoverable_without_reused_nonce(self):
        event=EventStore(':memory:');self.addCleanup(event.close)
        def rejected(rows):raise PermanentProviderReject('synthetic HTTP 401 refusal')
        result=process_event(event,token='rejected',claimed_venue='venue_a',authenticated_venue='venue_a',
            recipient_key='guest@example.test',preferences=self.prefs,sender=rejected,now=1.)
        self.assertEqual(result,'permanent_failure')
        attempts=event.summary('rejected')[1]
        event.reconcile('rejected',now=40,decision='confirmed_not_sent',
            evidence='Provider definitively rejected token; credentials repaired')
        self.assertEqual(event.summary('rejected')[0],'pending')
        next_claim=event.claim('rejected',fingerprint_for('venue_a','guest@example.test',self.prefs),now=41)
        self.assertEqual(next_claim.attempt,attempts+1)
        self.assertEqual(next_claim.status,'claimed')

    def test_a17_06_uncertain_submission_not_operator_requeued(self):
        event=EventStore(':memory:');self.addCleanup(event.close)
        def uncertain(rows):raise SubmissionUncertain('provider response timed out')
        result=process_event(event,token='uncertain',claimed_venue='venue_a',authenticated_venue='venue_a',
            recipient_key='guest@example.test',preferences=self.prefs,sender=uncertain,now=1.)
        self.assertEqual(result,'reconciliation_required')
        with self.assertRaisesRegex(ValueError,'Cannot requeue'):
            event.reconcile('uncertain',now=50,decision='confirmed_not_sent',evidence='No matching dashboard record observed')

    def test_a17_07_unencodable_tokens_rejected_before_hashing(self):
        store=EventStore(':memory:');self.addCleanup(store.close)
        for bad in ('bad\ud800','bad\u202e','bad\x00',' wrong','bad\n'):
            with self.subTest(token=repr(bad)),self.assertRaises(InputError):
                store.enqueue_request(token=bad,venue='venue_a',recipient='guest@example.test',preferences=self.prefs,now=1.)
        self.assertEqual(store.summary('valid'),None)

    def test_a17_07_signed_invalid_token_returns_http_400(self):
        store=EventStore(':memory:');self.addCleanup(store.close)
        binding=FormBinding('f','venue_a',b'synthetic-secret-signing-key-123')
        payload={'event_type':'form_response','form_response':{'form_id':'f','hidden':{'restaurant_id':'venue_a'},
            'token':'bad\ud800','submitted_at':'1970-01-01T00:01:40Z',
            'answers':[{'type':'email','email':'guest@example.test'}],
            'variables':[{'key':'wine_category','type':'text','text':'red'},
                         {'key':'sweetness','type':'text','text':'dry'}]}}
        raw=json.dumps(payload).encode('utf-8')
        signature='sha256='+base64.b64encode(hmac.new(binding.secret,raw,hashlib.sha256).digest()).decode()
        with self.assertRaises(InputError):verify_and_parse(raw,signature,binding,now=100)
        codes=[]
        WebhookApp(store,{'test':binding},clock=lambda:100)(
            {'REQUEST_METHOD':'POST','PATH_INFO':'/webhook/test','CONTENT_TYPE':'application/json',
             'CONTENT_LENGTH':str(len(raw)),'HTTP_TYPEFORM_SIGNATURE':signature,'wsgi.input':io.BytesIO(raw)},
            lambda status,headers:codes.append(status))
        self.assertTrue(codes[0].startswith('400'),codes)

if __name__=='__main__':unittest.main()
