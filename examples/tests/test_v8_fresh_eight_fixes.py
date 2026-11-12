"""Independent regression probes for eight v7 findings; synthetic fixtures only."""
from __future__ import annotations
from claim_fixture import claim_nonce
import base64
from datetime import datetime,timezone
import hashlib
import hmac
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock

BASE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BASE))
from webhook import FormBinding,verify_and_parse,MAX_SUBMISSION_AGE_SECONDS
from webapp import WebhookApp
from delivery import EventStore,Preferences,fingerprint_for,SubmissionUncertain,process_event,TokenCollision
from pipeline import InputError,CatalogDataError,load_catalog,MAX_CATALOG_ROWS
from catalog_store import CatalogStore
from postgres_catalog import extract_reviewable_pairings,PostgresCatalogStore
from providers import render_email

class EightFreshFixes(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        root=Path(self.tmp.name)
        self.events=EventStore(root/'events.sqlite');self.addCleanup(self.events.close)
        self.catalog=CatalogStore(root/'catalog.sqlite');self.addCleanup(self.catalog.close)
        self.catalog.replace_venue('venue_a',[w for w in load_catalog() if w.restaurant_id=='venue_a'])
        self.prefs=Preferences(category='red',sweetness='dry')
    def _signed(self,submitted_at,token='t'):
        binding=FormBinding('form-v8','venue_a',b'synthetic-v8-signature-secret')
        payload={'event_type':'form_response','form_response':{
            'form_id':'form-v8','token':token,'submitted_at':submitted_at,
            'hidden':{'restaurant_id':'venue_a'},'answers':[{'type':'email','email':'a@example.test'}],
            'variables':[{'key':'wine_category','type':'text','text':'red'},
                         {'key':'sweetness','type':'text','text':'dry'}]}}
        raw=json.dumps(payload).encode()
        sig='sha256='+base64.b64encode(hmac.new(binding.secret,raw,hashlib.sha256).digest()).decode()
        return raw,sig,binding
    def test_p7_01_expired_tombstone_replay_rejected_by_freshness(self):
        raw,sig,b=self._signed('1970-01-01T00:01:40Z')
        self.assertEqual(verify_and_parse(raw,sig,b,now=100).venue_id,'venue_a')
        with self.assertRaisesRegex(InputError,'Stale'):
            verify_and_parse(raw,sig,b,now=100+MAX_SUBMISSION_AGE_SECONDS+1)
        app=WebhookApp(self.events,{'a':b},clock=lambda:100+400*86400)
        statuses=[]
        app({'REQUEST_METHOD':'POST','PATH_INFO':'/webhook/a','CONTENT_TYPE':'application/json',
             'CONTENT_LENGTH':str(len(raw)),'HTTP_TYPEFORM_SIGNATURE':sig,'wsgi.input':io.BytesIO(raw)},
            lambda s,h:statuses.append(s))
        self.assertTrue(statuses[0].startswith('400'))
        self.assertIsNone(self.events.summary('t'))
    def test_p7_01_future_and_missing_submission_timestamps_rejected(self):
        raw,sig,b=self._signed('1970-01-01T00:06:41Z')
        with self.assertRaisesRegex(InputError,'Stale or future'):verify_and_parse(raw,sig,b,now=100)
        raw,sig,b=self._signed('1970-01-01T00:01:40')
        with self.assertRaisesRegex(InputError,'Invalid signed'):verify_and_parse(raw,sig,b,now=100)
        raw,sig,b=self._signed('1970-01-01T00:01:40Z')
        with self.assertRaises(PermissionError):verify_and_parse(raw,sig+'A',b,now=100)
    def test_p7_02_stale_hold_from_claim_is_idempotent(self):
        self.events.enqueue_request(token='race',venue='venue_a',recipient='a@example.test',preferences=self.prefs,now=100)
        fp=fingerprint_for('venue_a','a@example.test',self.prefs)
        claim=self.events.claim('race',fp,now=100)
        self.events.mark_dispatch_started('race',claim.attempt,now=101,nonce=claim_nonce(self.events,'race',claim.attempt))
        second=self.events.claim('race',fp,now=200)
        self.assertEqual(second.status,'reconciliation_required')
        self.events.mark_uncertain('race',claim.attempt,now=201,nonce=claim.nonce)
        self.assertEqual(self.events.summary('race')[0],'reconciliation_required')
        with self.assertRaises(RuntimeError):
            self.events.mark_uncertain('race',claim.attempt,now=202,nonce='f'*32)
    def test_p7_03_sqlite_rejects_invalid_tag_members_on_update_and_insert(self):
        wine=self.catalog.wines_for_venue('venue_a')[0]
        with self.catalog._connect() as conn:
            for column,bad in (('characteristics','[12]'),('pairings','[null]'),
                               ('characteristics','["INVALID"]'),('pairings','[""]'),
                               ('pairings','["foo@@"]')):
                with self.subTest(column=column,bad=bad),self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(f'UPDATE catalog SET {column}=? WHERE restaurant_id=? AND wine_id=?',
                                 (bad,'venue_a',wine.wine_id))
        self.assertEqual(self.catalog.wines_for_venue('venue_a')[0].wine_id,wine.wine_id)
    def test_p7_04_bounded_server_side_import(self):
        # Cursor fake rejects accidental unbounded fetchall before writing.
        from test_v3_postgres_contract import FakePG
        row=(1,'Example Cabernet',True,False,'Origin','red','cabernet','fruit','full','dry','Pairs with steak',None)
        pg=FakePG(legacy_rows=(row,))
        def bounded_fetch(size):
            self.assertEqual(size,MAX_CATALOG_ROWS+1)
            return list(pg.current[:size])
        pg.fetchmany=bounded_fetch
        pg.fetchall=lambda: (_ for _ in ()).throw(AssertionError('unbounded fetchall')) if pg.current == [row] else list(pg.current)
        result=PostgresCatalogStore(lambda:pg).import_legacy()
        self.assertEqual(result,{'legacy-1':1})
        self.assertIn({'name':'palate_legacy_bounded_import'},pg.cursor_calls)
    def test_p7_04_aborts_oversized_before_replacing(self):
        from test_v3_postgres_contract import FakePG
        row=(1,'Example Cabernet',True,False,'Origin','red','cabernet','fruit','full','dry','Pairs with steak',None)
        pg=FakePG(legacy_rows=(row,))
        pg.fetchmany=lambda size: [row]*(MAX_CATALOG_ROWS+1) if pg.current == [row] else list(pg.current[:size])
        with self.assertRaisesRegex(CatalogDataError,'legacy inventory size'):
            PostgresCatalogStore(lambda:pg).import_legacy()
        self.assertFalse(any(q.startswith('delete from portfolio_v2.wines') for q,_ in pg.queries))
    def test_p7_05_contractions_and_negation_excluded(self):
        for message in ("Doesn't pair with steak",'Not good with steak',
                        "Don't serve with lobster",'Excellent with fish, but not suitable with steak'):
            with self.subTest(message=message):self.assertFalse(extract_reviewable_pairings(message))
        self.assertIn('red_meat',extract_reviewable_pairings('Best paired with steak'))
    def test_p7_06_email_displays_human_readable_type(self):
        sample={'wine_id':'fake-id','wine_name':'Synthetic One','pronunciation':'Test',
                'origin':'Fictional Valley','type':'pinot_noir','description':'Example'}
        body=render_email([sample,{**sample,'wine_name':'Synthetic Two'},{**sample,'wine_name':'Synthetic Three'}])
        self.assertIn('Pinot Noir',body)
        self.assertNotIn('pinot_noir',body)
    def test_p7_07_unresolved_limit_enforced(self):
        for value in (-1,0,501,True,False,'10',1.5):
            with self.subTest(value=value),self.assertRaises(ValueError):self.events.unresolved(value)
        self.assertEqual(self.events.unresolved(1),())
    def test_p7_08_docs_do_not_permit_requeue_of_submitted_event(self):
        guide=(BASE/'README.md').read_text()
        self.assertIn('Manual requeue is forbidden for any event',guide)
        self.assertNotIn('Before manual requeue',guide)

if __name__=='__main__':unittest.main()
