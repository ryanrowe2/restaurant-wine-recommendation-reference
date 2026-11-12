"""Independent v15 regressions for v14 audit; synthetic inputs, no network."""
from __future__ import annotations
import json
from contextlib import closing
import re
import sqlite3
import struct
import sys
import tempfile
import unicodedata
import unittest
import zipfile
import zlib
from dataclasses import replace
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'examples'),str(ROOT/'scripts')]
from delivery import EventStore,fingerprint_for,MAX_DELIVERY_ATTEMPTS
from pipeline import Wine,Preferences,CatalogDataError,valid_required_wine_text,load_catalog,select_candidates,create_deterministic_recommendations,validate_recommendation_contract
from catalog_store import CatalogStore,MAX_TAG_JSON_BYTES
from release_gate import scan,scan_distribution
from postgres_catalog import _validate_schema_metadata,_assert_required_text_updates_enforced
from test_v13_five_finding_fixes import valid_schema


class V15FiveFindings(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory(prefix='palate-v15-')
        self.addCleanup(tmp.cleanup)
        self.tmp=Path(tmp.name)
        self.wines=[w for w in load_catalog() if w.restaurant_id=='venue_a']

    def _event(self,token='fence'):
        store=EventStore(self.tmp/(token+'.sqlite'))
        self.addCleanup(store.close)
        p=Preferences(category='red',sweetness='dry')
        store.enqueue_request(token=token,venue='venue_a',recipient='diner@example.org',preferences=p,now=1)
        return store,fingerprint_for('venue_a','diner@example.org',p)

    def test_p14_01_requeue_keeps_monotonic_attempt_and_rejects_stale_worker(self):
        store,fp=self._event()
        original=store.claim('fence',fp,now=2)
        store.mark_permanent_failure('fence',original.attempt,now=3,nonce=original.nonce)
        store.reconcile('fence',now=4,decision='confirmed_not_sent',evidence='pre-dispatch failure reviewed')
        newer=store.claim('fence',fp,now=5)
        self.assertGreater(newer.attempt,original.attempt)
        self.assertNotEqual(newer.nonce,original.nonce)
        for transition in (lambda:store.heartbeat('fence',original.attempt,now=6),
                           lambda:store.mark_dispatch_started('fence',original.attempt,now=6),
                           lambda:store.mark_permanent_failure('fence',original.attempt,now=6),
                           lambda:store.mark_dispatch_started('fence',newer.attempt,now=6,nonce=original.nonce)):
            with self.subTest(transition=transition),self.assertRaises(RuntimeError):transition()
        store.heartbeat('fence',newer.attempt,now=6,nonce=newer.nonce)
        store.mark_dispatch_started('fence',newer.attempt,now=6,nonce=newer.nonce)
        store.mark_provider_accepted('fence',newer.attempt,'receipt',now=7,nonce=newer.nonce)
        self.assertEqual(store.summary('fence'),('provider_accepted',2,'receipt'))

    def test_p14_01_operator_requeue_gets_fresh_budget_without_reusing_claim_identity(self):
        store,fp=self._event()
        for i in range(1,MAX_DELIVERY_ATTEMPTS+1):
            claim=store.claim('fence',fp,now=i*100)
            self.assertEqual(claim.attempt,i)
            state=store.mark_retryable_failure('fence',claim.attempt,now=i*100+1,nonce=claim.nonce)
        self.assertEqual(state,'retry_exhausted')
        store.reconcile('fence',now=350,decision='confirmed_not_sent',evidence='no submission ever attempted')
        next_claim=store.claim('fence',fp,now=351)
        self.assertEqual(next_claim.attempt,MAX_DELIVERY_ATTEMPTS+1)
        self.assertEqual(store.mark_retryable_failure('fence',next_claim.attempt,now=352,nonce=next_claim.nonce),'retryable_failure')
        with store._connect() as conn:
            self.assertEqual(conn.execute('SELECT attempts,cycle_attempts FROM events WHERE token=?',('fence',)).fetchone(),(4,1))

    def test_p14_01_legacy_database_migrates_without_resetting_attempts(self):
        path=self.tmp/'old.sqlite'
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("""CREATE TABLE events(token TEXT PRIMARY KEY, fingerprint TEXT, request_json TEXT,
              state TEXT, attempts INTEGER NOT NULL, lease_until REAL, next_retry_at REAL,delivery_ref TEXT,
              last_error TEXT,created_at REAL NOT NULL,updated_at REAL NOT NULL,dispatch_nonce TEXT)""")
            db.execute("INSERT INTO events(token,fingerprint,state,attempts,created_at,updated_at) VALUES('old','fp','pending',2,1,1)")
        with self.assertRaisesRegex(ValueError,'schema drift'):
            EventStore(path)
        with closing(sqlite3.connect(path)) as db:
            self.assertEqual(db.execute("SELECT attempts FROM events WHERE token='old'").fetchone(),(2,))

    def test_p14_02_text_domain_rejects_control_and_unicode_whitespace(self):
        for bad in ('Bad\x1fWine','\x00','\t','\n','\u00a0','Name\u2028Next','\x85','Bad\x7f'):
            with self.subTest(bad=repr(bad)):
                self.assertFalse(valid_required_wine_text(bad))
                with self.assertRaises(CatalogDataError):
                    replace(self.wines[0],wine_name=bad)
        for good in ('Wine', 'Red Wine','Élevé 2024'):
            self.assertTrue(valid_required_wine_text(good))

    def test_p14_02_direct_sql_and_recommendation_roundtrip(self):
        db=CatalogStore(self.tmp/'catalog.sqlite')
        self.addCleanup(db.close)
        db.replace_venue('venue_a',self.wines)
        for bad in ('Bad\x1fWine','\u00a0','Name\tNext'):
            with self.subTest(bad=repr(bad)), self.assertRaises(sqlite3.DatabaseError),db._connect() as conn:
                conn.execute('UPDATE catalog SET wine_name=? WHERE wine_id=? AND restaurant_id=?',
                             (bad,self.wines[0].wine_id,'venue_a'))
        reloaded=db.wines_for_venue('venue_a')
        prefs=Preferences(category='red',sweetness='dry')
        selection=select_candidates(reloaded,'venue_a',prefs)
        output=create_deterministic_recommendations(selection,prefs)
        validate_recommendation_contract(output,selection.wines,'venue_a')

    def test_p14_03_oversized_raw_json_fails_closed_without_oversize_persist(self):
        db=CatalogStore(self.tmp/'bounded.sqlite')
        self.addCleanup(db.close)
        db.replace_venue('venue_a',[self.wines[0]])
        valid=json.dumps([f'tag_{i:03d}' for i in range(128)])
        self.assertLessEqual(len(valid),MAX_TAG_JSON_BYTES)
        self.assertEqual(db._valid_tag_array(valid),1)
        invalid='["bold"'+' '*1_000_000+']'
        self.assertEqual(db._valid_tag_array(invalid),0)
        with self.assertRaises(sqlite3.DatabaseError),db._connect() as conn:
            conn.execute('UPDATE catalog SET characteristics=? WHERE restaurant_id=?',(invalid,'venue_a'))
        self.assertEqual(len(db.wines_for_venue('venue_a')),1)

    def test_p14_03_corrupt_oversized_prior_tags_block_database_upgrade(self):
        path=self.tmp/'old_oversize.sqlite'
        old=CatalogStore(path)
        self.addCleanup(old.close)
        old.replace_venue('venue_a',[self.wines[0]])
        with old._connect() as conn:
            conn.execute('DROP TRIGGER catalog_validate_tags_update')
            conn.execute('UPDATE catalog SET characteristics=? WHERE restaurant_id=?',
                         ('["bold"'+' '*9000+']','venue_a'))
        with self.assertRaisesRegex(CatalogDataError,'invalid tags'):
            CatalogStore(path)

    @staticmethod
    def _png_with_synthetic_secret():
        def chunk(kind,data):
            return struct.pack('!I',len(data))+kind+data+struct.pack('!I',zlib.crc32(kind+data))
        fake='sk-proj-'+'a'*40
        return (b'\x89PNG\r\n\x1a\n'+
                chunk(b'IHDR',struct.pack('!2I5B',1,1,8,2,0,0,0))+
                chunk(b'tEXt',b'Comment\0'+fake.encode())+
                chunk(b'IDAT',zlib.compress(b'\x00\xff\xff\xff'))+
                chunk(b'IEND',b''))

    def test_p14_04_raster_metadata_credential_is_blocked_by_release_scanner(self):
        root=self.tmp/'repo';(root/'diagrams').mkdir(parents=True)
        (root/'diagrams'/'hidden.png').write_bytes(self._png_with_synthetic_secret())
        self.assertTrue(any('raster media not allowed' in error for error in scan(root)))
        archive=self.tmp/'candidate.zip'
        with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as z:
            z.write(root/'diagrams'/'hidden.png','restaurant-wine-recommendation-reference/diagrams/hidden.png')
        self.assertTrue(any('raster media not allowed' in error for error in scan_distribution(archive)))

    def test_p14_04_other_raster_formats_are_conservatively_rejected(self):
        root=self.tmp/'repo';(root/'diagrams').mkdir(parents=True)
        for ext in ('jpg','jpeg','webp'):(root/'diagrams'/('raw.'+ext)).write_bytes(b'not-an-approved-rendering')
        errors=scan(root)
        self.assertEqual(sum('raster media not allowed' in e for e in errors),3)

    def test_p14_05_additive_sql_contract_covers_unicode_whitespace_and_controls(self):
        sql=(ROOT/'examples/migrations/007_required_text_contract.sql').read_text()
        forbidden={int(c) for c in re.findall(r'chr\((\d+)\)',sql)}
        expected={i for i in range(1,0x110000) if unicodedata.category(chr(i))=='Cc' or
                  (chr(i).isspace() and i!=32)}
        self.assertEqual(forbidden,expected)
        self.assertIn('CREATE FUNCTION portfolio_v2.valid_required_wine_text',sql)
        self.assertIn('ADD CONSTRAINT wine_required_text_contract_v15',sql)
        for field in ('wine_id','wine_name','pronunciation','origin','wine_type'):
            self.assertIn(f'valid_required_wine_text({field})',sql)
        self.assertNotIn('valid_required_wine_text',(ROOT/'examples/migrations/004_required_wine_fields.sql').read_text())

    def test_p14_05_schema_verifier_rejects_missing_required_constraint(self):
        columns,constraints,indexes=valid_schema()
        _validate_schema_metadata(columns,constraints,indexes)
        old=[c for c in constraints if c[1]!='wine_required_text_contract_v15']
        with self.assertRaisesRegex(RuntimeError,'v15 required-text'):
            _validate_schema_metadata(columns,old,indexes)

    def test_p14_05_transactional_update_negative_controls(self):
        class ConstraintFailure(Exception):pgcode='23514'
        class Probe:
            def __init__(self,permit_bad=False):self.calls=[];self.permit_bad=permit_bad
            def execute(self,sql,params=None):
                self.calls.append((sql,params))
                if sql.startswith('UPDATE portfolio_v2.wines SET') and not self.permit_bad:
                    raise ConstraintFailure('simulated constraint failure')
        probe=Probe()
        _assert_required_text_updates_enforced(probe)
        self.assertEqual(sum(q.startswith('UPDATE portfolio_v2.wines') for q,_ in probe.calls),5)
        self.assertEqual(sum(q=='ROLLBACK TO SAVEPOINT palate_required_update_case' for q,_ in probe.calls),5)
        with self.assertRaisesRegex(RuntimeError,'invalid required-text UPDATE accepted'):
            _assert_required_text_updates_enforced(Probe(permit_bad=True))


if __name__=='__main__':unittest.main()
