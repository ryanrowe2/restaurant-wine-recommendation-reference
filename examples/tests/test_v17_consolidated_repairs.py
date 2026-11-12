"""Adversarial regression for all twelve 2026-11-12 root-cause findings.

Only synthetic records, disposable SQLite DBs, and simulated external services.
All PostgreSQL assertions are explicitly OFFLINE until tested on a live server.
"""
from __future__ import annotations
import hashlib
import json
import os
import sqlite3
import stat
import sys
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'examples'),str(ROOT/'scripts'),str(Path(__file__).parent)]
from pipeline import (Preferences,InputError,CatalogDataError,ContractError,load_catalog,
                      select_candidates,create_deterministic_recommendations)
from delivery import EventStore,process_event,fingerprint_for
from catalog_store import CatalogStore
from postgres_catalog import (_validate_schema_metadata,restored_pronunciation,PostgresCatalogStore,
                              _assert_quarantine_table_control)
from release_gate import scan
from test_v13_five_finding_fixes import valid_schema
from test_v3_postgres_contract import FakePG

class ConsolidatedRepairs(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory(prefix='palate-v17-')
        self.addCleanup(temp.cleanup)
        self.temp=Path(temp.name)
        self.prefs=Preferences(category='red',sweetness='dry')

    def admitted(self,*,token='admitted',db=None,now=1):
        store=db or EventStore(self.temp/(token+'.sqlite'))
        if db is None:self.addCleanup(store.close)
        store.enqueue_request(token=token,venue='venue_a',recipient='guest@example.test',
                              preferences=self.prefs,now=now)
        return store,fingerprint_for('venue_a','guest@example.test',self.prefs)

    def test_a16_01_xml_references_and_nested_html_entities_are_scanned_after_parsing(self):
        root=self.temp/'source';(root/'diagrams').mkdir(parents=True)
        token='sk-proj-'+'q'*40
        for encoded in ('&#x73;&#x6b;'+token[2:],
                        '&amp;#115;&amp;#107;'+token[2:]):
            svg='<svg xmlns="http://www.w3.org/2000/svg"><desc>'+encoded+'</desc></svg>'
            (root/'diagrams'/'diagram.svg').write_text(svg)
            issues=scan(root)
            self.assertTrue(any('decoded SVG metadata' in issue for issue in issues),issues)
        (root/'diagrams'/'diagram.svg').write_text('<svg xmlns="http://www.w3.org/2000/svg"><desc>Example chart</desc></svg>')
        self.assertEqual(scan(root),[])

    def test_a16_02_weakened_catalog_table_fails_closed_with_existing_data(self):
        db=self.temp/'catalog.sqlite'
        with closing(sqlite3.connect(db)) as con,con:
            con.execute('CREATE TABLE restaurants(restaurant_id TEXT PRIMARY KEY)')
            con.execute('CREATE TABLE catalog(restaurant_id TEXT,wine_id TEXT,category TEXT,available_by_glass INTEGER)')
            con.execute("INSERT INTO restaurants VALUES('venue_a')")
            con.execute("INSERT INTO catalog VALUES('venue_a','x','invalid_category',2)")
        with self.assertRaisesRegex(ValueError,'SQLite schema drift'):
            CatalogStore(db)
        with closing(sqlite3.connect(db)) as con,con:
            self.assertEqual(con.execute('SELECT category FROM catalog').fetchone(),('invalid_category',))

    @unittest.skipUnless(os.name=='posix','POSIX filesystem modes required')
    def test_a16_03_event_db_world_readable_mode_is_detected_on_every_connect(self):
        db=self.temp/'events.sqlite';store=EventStore(db);self.addCleanup(store.close)
        os.chmod(db,0o644)
        try:
            with self.assertRaisesRegex(ValueError,'permissions'):
                store.summary('nothing')
        finally:
            os.chmod(db,0o600)
        self.assertIsNone(store.summary('nothing'))

    @unittest.skipUnless(os.name=='posix','POSIX filesystem modes required')
    def test_a16_03_wal_sidecar_mode_is_checked(self):
        db=self.temp/'events.sqlite';store=EventStore(db);self.addCleanup(store.close)
        wal=Path(str(db)+'-wal')
        wal.write_bytes(b'synthetic-file')
        os.chmod(wal,0o644)
        with self.assertRaisesRegex(ValueError,'auxiliary permissions'):
            store.summary('none')
        wal.unlink()
        self.assertIsNone(store.summary('none'))

    def test_a16_04_invalid_venue_rejected_before_admission_or_quota_writes(self):
        store=EventStore(':memory:');self.addCleanup(store.close)
        for venue in ('\u200b','\u202eBAD','v'*129):
            with self.subTest(venue=repr(venue)),self.assertRaises(InputError):
                store.enqueue_request(token='bad',venue=venue,recipient='guest@example.test',preferences=self.prefs,now=1)
        with store._connect() as c:
            self.assertEqual(c.execute('SELECT count(*) FROM events').fetchone()[0],0)
            self.assertEqual(c.execute('SELECT count(*) FROM admission_ledger').fetchone()[0],0)

    def test_a16_05_postgres_additive_tag_function_rejects_noncanonical_arrays(self):
        text=(ROOT/'examples/migrations/009_canonical_postgres_arrays.sql').read_text()
        self.assertIn('CREATE OR REPLACE FUNCTION portfolio_v2.valid_tag_array',text)
        self.assertIn('WITH ORDINALITY',text)
        self.assertIn('prior.ordinal=t.ordinal-1',text)
        self.assertIn('COLLATE "C"',text)
        self.assertIn('wine_review_archive',text)
        self.assertIn('Historical noncanonical tag arrays require explicit review',text)
        # Prior migrations are not rewritten; this new function runs via migrate_schema.
        old=[p.name for p in (ROOT/'examples/migrations').glob('00[1-8]_*.sql')]
        self.assertEqual(len(old),8)

    def test_f16_01_unverified_custom_recommender_text_cannot_reach_sender(self):
        store,_=self.admitted(token='model')
        dispatched=[]
        def made_up(selection,preferences):
            output=create_deterministic_recommendations(selection,preferences)
            output[0]['description']='Cures illness and costs exactly $1 per bottle.'
            return output
        state=process_event(store,token='model',claimed_venue='venue_a',authenticated_venue='venue_a',
                            recipient_key='guest@example.test',preferences=self.prefs,now=2,
                            recommender=made_up,sender=lambda rows:(dispatched.append(rows) or 'synthetic-provider-id'))
        self.assertEqual(state,'provider_accepted')
        self.assertEqual(len(dispatched),1)
        self.assertFalse(any('Cures illness' in row['description'] for row in dispatched[0]))
        with store._connect() as con:
            self.assertGreaterEqual(con.execute("SELECT count(*) FROM event_audit WHERE category='model_degraded:contract'").fetchone()[0],1)

    def test_f16_02_claim_never_creates_an_unadmitted_event(self):
        store=EventStore(':memory:');self.addCleanup(store.close)
        with self.assertRaisesRegex(InputError,'verified durable admission'):
            store.claim('forged','any-string',now=2)
        self.assertIsNone(store.summary('forged'))
        with store._connect() as con:
            self.assertEqual(con.execute('SELECT count(*) FROM events').fetchone()[0],0)
            con.execute("INSERT INTO events(token,fingerprint,request_json,state,created_at,updated_at) VALUES ('forged','hash','{}','pending',1,1)")
        with self.assertRaisesRegex(InputError,'admission ledger'):
            store.claim('forged','hash',now=2)
        self.assertEqual(store.summary('forged')[0],'pending')

    def test_f16_02_tampered_durable_admission_cannot_dispatch(self):
        store,fp=self.admitted(token='tampered')
        with store._connect() as con:
            con.execute("UPDATE admission_ledger SET venue='venue_b' WHERE token='tampered'")
        with self.assertRaisesRegex(InputError,'admission ledger'):
            store.claim('tampered',fp,now=3)

    def test_f16_03_stale_poison_cannot_destroy_live_worker_claim(self):
        store,fp=self.admitted(token='active')
        first=store.claim('active',fp,now=2)
        with self.assertRaisesRegex(RuntimeError,'Active worker claim'):
            store.poison('active',now=3)
        self.assertEqual(store.summary('active')[0],'sending')
        store.mark_dispatch_started('active',first.attempt,now=4,nonce=first.nonce)
        store.mark_provider_accepted('active',first.attempt,'provider-confirmed',now=5,nonce=first.nonce)
        self.assertEqual(store.summary('active')[0],'provider_accepted')

    def test_f16_04_existing_event_schema_without_checks_is_rejected_before_alter(self):
        db=self.temp/'old_event.sqlite'
        with closing(sqlite3.connect(db)) as con,con:
            con.execute('''CREATE TABLE events(token TEXT PRIMARY KEY,fingerprint TEXT,
                        request_json TEXT,state TEXT,attempts INTEGER NOT NULL,
                        lease_until REAL,next_retry_at REAL,created_at REAL,updated_at REAL)''')
            con.execute("INSERT INTO events(token,fingerprint,state,attempts,created_at,updated_at) VALUES('old','f','invalid_state',-1,1,1)")
        with self.assertRaisesRegex(ValueError,'SQLite schema drift'):
            EventStore(db)
        with closing(sqlite3.connect(db)) as con,con:
            self.assertEqual(con.execute("SELECT state,attempts FROM events WHERE token='old'").fetchone(),('invalid_state',-1))
            self.assertNotIn('cycle_attempts',[r[1] for r in con.execute('PRAGMA table_info(events)')])

    def test_f16_05_legacy_import_skips_quarantined_pronunciation(self):
        row=(1,'Example Cabernet',True,False,'Origin','red','cabernet','fruit','full','dry','Pairs with steak',None)
        wine_id='stable-id'
        pg=FakePG(legacy_rows=(row,),aliases=(('example cabernet',wine_id),),
                  old=((wine_id,'Example Cabernet','Pronunciation requires review',[],[],False),))
        pg.archive=[(wine_id,'Pronunciation requires review',[],[],False,None,None,None,None,[])]
        original=pg.execute
        def scripted(query,params=None):
            original(query,params)
            if 'FROM portfolio_v2.wine_review_archive WHERE' in query:
                pg.current=pg.archive
        pg.execute=scripted
        store=PostgresCatalogStore(lambda:pg)
        store.import_legacy()
        args=[params for sql,params in pg.queries if sql.startswith('insert into portfolio_v2.wines')]
        self.assertEqual(len(args),1)
        self.assertEqual(args[0][3],'Pronunciation not provided')
        self.assertTrue(any('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ' in q.upper() for q,_ in pg.queries))
        self.assertEqual(restored_pronunciation('Pronunciation requires review','Reviewed'), 'Reviewed')

    def test_f16_07_worker_id_length_and_population_bounded(self):
        store=EventStore(':memory:');self.addCleanup(store.close)
        for invalid in ('x'*65,'bad\nname','\u200b',None,42):
            with self.subTest(worker=repr(invalid)),self.assertRaises(InputError):
                store.worker_heartbeat(now=10,worker_id=invalid)
        with self.subTest():
            for index in range(1024):
                store.worker_heartbeat(now=10,worker_id=f'worker-{index}')
            with self.assertRaisesRegex(InputError,'cardinality'):
                store.worker_heartbeat(now=11,worker_id='worker-1024')
            store.worker_heartbeat(now=86412,worker_id='worker-1024')
        with store._connect() as c:
            self.assertEqual(c.execute('SELECT count(*) FROM worker_status').fetchone()[0],1)

    def test_f16_09_quarantine_table_columns_and_primary_key_verified(self):
        cols,constraints,indexes=valid_schema()
        _validate_schema_metadata(cols,constraints,indexes)
        bad_cols=[row for row in cols if row[:2]!=('archive_text_quarantine_v16','reason')]
        with self.assertRaisesRegex(RuntimeError,'quarantine|reason'):
            _validate_schema_metadata(bad_cols,constraints,indexes)
        bad_keys=[row for row in constraints if row[:2]!=('archive_text_quarantine_v16','archive_text_quarantine_v16_pkey')]
        with self.assertRaisesRegex(RuntimeError,'primary key'):
            _validate_schema_metadata(cols,bad_keys,indexes)

    def test_f16_09_quarantine_readback_probe_rejects_silent_corruption(self):
        class Cursor:
            def __init__(self,mutated=False):self.calls=[];self.mutated=mutated;self.saved=None
            def execute(self,sql,params=None):
                self.calls.append(sql.strip())
                if sql.lstrip().startswith('INSERT INTO portfolio_v2.archive_text_quarantine_v16'):
                    self.saved=params
            def fetchone(self):
                return ('tampered',self.saved[3]) if self.mutated else self.saved[2:]
        c=Cursor()
        _assert_quarantine_table_control(c)
        self.assertIn('ROLLBACK TO SAVEPOINT palate_quarantine_audit',c.calls)
        broken=Cursor(True)
        with self.assertRaisesRegex(RuntimeError,'quarantine evidence'):
            _assert_quarantine_table_control(broken)
        self.assertEqual(broken.calls[-2:],['ROLLBACK TO SAVEPOINT palate_quarantine_audit',
                                           'RELEASE SAVEPOINT palate_quarantine_audit'])

if __name__=='__main__':unittest.main()
