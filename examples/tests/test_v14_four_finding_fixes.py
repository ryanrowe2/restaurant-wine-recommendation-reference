"""Independent, executable regressions for the four v13 defects. Synthetic data only."""
from __future__ import annotations
from claim_fixture import claim_nonce
import json
from contextlib import closing
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'examples'))
sys.path.insert(0,str(Path(__file__).parent))
from catalog_store import CatalogStore
from pipeline import CatalogDataError,Preferences
from delivery import EventStore,fingerprint_for
from postgres_catalog import (_validate_schema_metadata,_assert_database_positive_controls)
from test_v13_five_finding_fixes import valid_schema


INSERT='''INSERT INTO catalog
(restaurant_id,wine_id,wine_name,pronunciation,origin,category,type,sweetness,
 characteristics,pairings,available_by_glass,available_by_bottle)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'''


def row(tags, *, wine_id='id-1'):
    return ('venue',wine_id,'Fictional Wine','Pronunciation','Imaginary','red','Fictional type',
            'dry',json.dumps(tags,ensure_ascii=True),'[]',1,0)


class PositiveProbeCursor:
    """Offline PostgreSQL driver double that persists 16 synthetic probes."""
    def __init__(self,disallow=None,mutate=None):
        self.calls=[]
        self.disallow=disallow
        self.mutate=mutate
        self.values={}
        self.latest=None
    def execute(self,sql,params=None):
        self.calls.append((sql,params))
        if self.disallow is not None and self.disallow(sql,params):
            raise RuntimeError('Simulated extra restrictive database constraint')
        action=sql.strip().upper()
        if action.startswith('INSERT INTO PORTFOLIO_V2.WINES'):
            venue,wine_id,name,pronunciation,origin,category,wine_type,sweetness,characteristics,pairings,suggested,glass,bottle=params
            self.values[venue,wine_id]=(name,pronunciation,origin,category,wine_type,sweetness,
                 json.loads(characteristics),json.loads(pairings),json.loads(suggested),glass,bottle)
        elif action.startswith('UPDATE PORTFOLIO_V2.WINES'):
            category,sweetness,glass,bottle,venue,wine_id=params
            previous=self.values[venue,wine_id]
            self.values[venue,wine_id]=(*previous[:3],category,previous[4],sweetness,
                                        *previous[6:9],glass,bottle)
        elif action.startswith('SELECT WINE_NAME,PRONUNCIATION'):
            self.latest=self.values.get(tuple(params))
            if self.mutate and self.latest:self.latest=self.mutate(self.latest)
    def fetchone(self):
        return self.latest


class V14FourFindings(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def make_catalog(self,name='wine.sqlite'):
        store=CatalogStore(Path(self.tmp.name)/name)
        self.addCleanup(store.close)
        with store._connect() as conn:
            conn.execute('INSERT INTO restaurants(restaurant_id) VALUES (?)',('venue',))
        return store

    def test_p13_01_json_nul_and_noncanonical_tags_rejected_at_sql_boundary(self):
        store=self.make_catalog()
        invalid=(['bold\x00BAD'],['good\x00!'],['good\x00'],['bad!'],
                 ['MixedCase'],['two words'],[''],['x'*49],['valid']*129,
                 ['surrogate\ud800'],[12],['é'])
        for number,tags in enumerate(invalid):
            with self.subTest(tags=repr(tags)),store._connect() as conn:
                with self.assertRaises(sqlite3.DatabaseError):
                    conn.execute(INSERT,row(tags,wine_id='bad-'+str(number)))
        with store._connect() as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM catalog').fetchone()[0],0)
            conn.execute(INSERT,row(['bold','red_meat'],wine_id='good'))
            self.assertEqual(store._valid_tag_array(json.dumps([f'tag_{i:03d}' for i in range(128)])),1)
        self.assertEqual(store.wines_for_venue('venue')[0].characteristics,frozenset(('bold','red_meat')))
        with store._connect() as conn:
            with self.assertRaises(sqlite3.DatabaseError):
                conn.execute("UPDATE catalog SET pairings=? WHERE wine_id='good'",(json.dumps(['bad\x00tag']),))
        self.assertEqual(store.wines_for_venue('venue')[0].pairings,frozenset())
        # Any raw SQLite writer missing the validator must fail closed.
        with closing(sqlite3.connect(store._db.path)) as raw, raw:
            with self.assertRaisesRegex(sqlite3.OperationalError,'no such function'):
                raw.execute("UPDATE catalog SET characteristics='[]' WHERE wine_id='good'")

    def test_p13_03_existing_weak_triggers_replaced_atomically_on_upgrade(self):
        path=Path(self.tmp.name)/'upgrade.sqlite'
        previous=self.make_catalog('upgrade.sqlite')
        previous.close()
        with closing(sqlite3.connect(path)) as raw, raw:
            for action in ('insert','update'):
                raw.execute(f'DROP TRIGGER catalog_validate_tags_{action}')
                raw.execute(f'''CREATE TRIGGER catalog_validate_tags_{action}
                    BEFORE {action.upper()} ON catalog BEGIN SELECT 1; END''')
        upgraded=CatalogStore(path)
        self.addCleanup(upgraded.close)
        with upgraded._connect() as conn:
            conn.execute(INSERT,row(['valid'],wine_id='good'))
            with self.assertRaises(sqlite3.DatabaseError):
                conn.execute(INSERT,row(['bad!'],wine_id='bad'))
            with self.assertRaises(sqlite3.DatabaseError):
                conn.execute("UPDATE catalog SET characteristics=? WHERE wine_id='good'",
                             (json.dumps(['good\x00NUL']),))
        self.assertEqual(upgraded.wines_for_venue('venue')[0].wine_id,'good')

    def test_p13_03_corrupt_prior_rows_block_upgrade_without_partial_migration(self):
        path=Path(self.tmp.name)/'invalid_legacy.sqlite'
        first=self.make_catalog('invalid_legacy.sqlite')
        first.close()
        with first._connect() as conn:
            conn.execute('DROP TRIGGER catalog_validate_tags_insert')
            conn.execute('DROP TRIGGER catalog_validate_tags_update')
            conn.execute(INSERT,row(['corrupt!'],wine_id='bad'))
        with self.assertRaisesRegex(CatalogDataError,'invalid tags'):
            CatalogStore(path)
        # Rollback preserved the old trigger set and did not commit half an upgrade.
        with closing(sqlite3.connect(path)) as conn, conn:
            definitions={name for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'")}
            self.assertNotIn('catalog_validate_tags_insert',definitions)
            self.assertNotIn('catalog_validate_tags_update',definitions)
        # Operator repairs historical data; the next opening rebuilds both triggers.
        with first._connect() as conn:
            conn.execute("UPDATE catalog SET characteristics='[]' WHERE wine_id='bad'")
        repaired=CatalogStore(path)
        self.addCleanup(repaired.close)
        self.assertEqual(repaired.wines_for_venue('venue')[0].characteristics,frozenset())

    def make_event(self):
        events=EventStore(':memory:')
        self.addCleanup(events.close)
        prefs=Preferences(category='red',sweetness='dry')
        events.enqueue_request(token='signed-token',venue='venue',recipient='reader@example.com',preferences=prefs,now=1.)
        fp=fingerprint_for('venue','reader@example.com',prefs)
        first=events.claim('signed-token',fp,now=1.,lease_seconds=1.)
        events.mark_dispatch_started('signed-token',first.attempt,now=1.1,nonce=claim_nonce(events,'signed-token',first.attempt))
        return events,first,fp

    def test_p13_02_expired_claim_late_receipt_accepted_with_original_nonce(self):
        events,first,fp=self.make_event()
        competing=events.claim('signed-token',fp,now=3.)
        self.assertEqual(competing.status,'reconciliation_required')
        events.mark_provider_accepted('signed-token',first.attempt,'provider-confirmed',now=3.1,nonce=first.nonce)
        self.assertEqual(events.summary('signed-token'),('provider_accepted',1,'provider-confirmed'))
        self.assertEqual(events.history('signed-token')[-1][1],'provider_accepted')
        with self.assertRaises(RuntimeError):
            events.mark_provider_accepted('signed-token',first.attempt,'duplicate',now=4.,nonce=first.nonce)
        self.assertEqual(events.claim('signed-token',fp,now=10.).status,'already_accepted')

    def test_p13_02_late_receipt_fenced_against_wrong_nonce_attempt_and_cancel(self):
        events,first,fp=self.make_event()
        self.assertEqual(events.claim('signed-token',fp,now=3.).status,'reconciliation_required')
        for attempt,nonce in ((first.attempt,'0'*32),(first.attempt+1,first.nonce)):
            with self.subTest(attempt=attempt,nonce=nonce),self.assertRaises(RuntimeError):
                events.mark_provider_accepted('signed-token',attempt,'forged',now=3.1,nonce=nonce)
        self.assertEqual(events.summary('signed-token')[0],'reconciliation_required')
        events.reconcile('signed-token',now=3.2,decision='cancel',evidence='Explicit operator cancellation')
        with self.assertRaises(RuntimeError):
            events.mark_provider_accepted('signed-token',first.attempt,'too-late',now=3.3,nonce=first.nonce)
        self.assertNotEqual(events.summary('signed-token')[0],'provider_accepted')

    def test_p13_04_unexpected_restrictions_and_partial_index_rejected(self):
        columns,constraints,indexes=valid_schema()
        _validate_schema_metadata(columns,constraints,indexes)
        constraints.append(('wines','unexpected_red_only','c',"CHECK (category = 'red')",True,(),None,()))
        with self.assertRaisesRegex(RuntimeError,'unexpected or restrictive'):
            _validate_schema_metadata(columns,constraints,indexes)
        columns,constraints,indexes=valid_schema()
        bad=[('wines','wines_venue_category',
              'CREATE INDEX wines_venue_category ON portfolio_v2.wines USING btree (venue_id, category, sweetness) WHERE false')]
        with self.assertRaisesRegex(RuntimeError,'index'):
            _validate_schema_metadata(columns,constraints,bad)
        bad=[('wines','wines_venue_category',
              'CREATE INDEX wines_venue_category ON portfolio_v2.wines USING btree (venue_id, category, sweetness) INCLUDE (wine_name)')]
        with self.assertRaisesRegex(RuntimeError,'index'):
            _validate_schema_metadata(columns,constraints,bad)

    def test_p13_04_all_supported_categories_and_sweetness_drilled_transactionally(self):
        probe=PositiveProbeCursor()
        _assert_database_positive_controls(probe)
        inserts=[p for sql,p in probe.calls if sql.lstrip().startswith('INSERT INTO portfolio_v2.wines')]
        updates=[p for sql,p in probe.calls if sql.lstrip().startswith('UPDATE portfolio_v2.wines')]
        self.assertEqual(len(inserts),16)
        self.assertEqual(len(updates),16)
        self.assertEqual({(p[5],p[7]) for p in inserts},
                         {(c,s) for c in ('red','white','sparkling','rose') for s in ('dry','semi-dry','semi-sweet','sweet')})
        self.assertEqual(probe.calls[0][0],'SAVEPOINT palate_positive_audit')
        self.assertEqual([call[0] for call in probe.calls[-2:]],
                         ['ROLLBACK TO SAVEPOINT palate_positive_audit','RELEASE SAVEPOINT palate_positive_audit'])
        reject=PositiveProbeCursor(lambda sql,params:
            sql.lstrip().startswith('INSERT INTO portfolio_v2.wines') and params[5]=='white')
        with self.assertRaisesRegex(RuntimeError,'supported wine category'):
            _assert_database_positive_controls(reject)
        self.assertEqual(reject.calls[-2][0],'ROLLBACK TO SAVEPOINT palate_positive_audit')
        reject_update=PositiveProbeCursor(lambda sql,params:
            sql.lstrip().startswith('UPDATE portfolio_v2.wines') and params[0]=='rose')
        with self.assertRaisesRegex(RuntimeError,'supported wine category'):
            _assert_database_positive_controls(reject_update)
        self.assertEqual(reject_update.calls[-2][0],'ROLLBACK TO SAVEPOINT palate_positive_audit')

if __name__=='__main__': unittest.main()
