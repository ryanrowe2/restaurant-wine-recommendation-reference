"""Adversarial v12 regressions for the six independently reported P11 findings.

No third-party credentials, actual recipients, or production databases are used.
Live PostgreSQL migrations/providers remain separate external acceptance gates.
"""
from __future__ import annotations

from dataclasses import replace
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'examples'),str(ROOT/'scripts')]
from delivery import SubmissionUncertain
from providers import PostmarkSender
from postgres_catalog import (
    PostgresCatalogStore,legacy_row_to_wine,_validate_schema_metadata,
    REQUIRED_SCHEMA_COLUMNS,NULLABLE_WINE_COLUMNS,
)
from catalog_store import CatalogStore
from pipeline import CatalogDataError,MAX_CATALOG_ROWS,load_catalog
from test_v3_postgres_contract import FakePG


class MockResponse:
    status=200
    def __init__(self,payload):self.buf=io.BytesIO(json.dumps(payload).encode())
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def read(self,n=-1):return self.buf.read(n)


class StatefulReviewPG(FakePG):
    """Fake PostgreSQL transaction contract holding active and archived rows.

    Assertions on emitted SQL ensure fake behavior mirrors the intended store
    operations. This deliberately is not represented as a live DB integration.
    """
    def __init__(self,wine,*,legacy=False):
        super().__init__(registry_ids=(1,) if legacy else (),aliases=((wine.wine_name.lower(),wine.wine_id),) if legacy else ())
        self.venue=wine.restaurant_id
        self.live={wine.wine_id:{
            'wine_name':wine.wine_name,'pronunciation':'Reviewed Pronunciation',
            'characteristics':['bold'],'pairings':['red_meat'],
            'pairings_reviewed':True,'source_metadata':'original source metadata',
            'source_characteristics':'original source traits',
            'source_body':'original source body',
            'source_pairings':'original source dish prose',
            'suggested_pairings':['red_meat'],
        }}
        self.archived={}
        self.inserted=[]

    def execute(self,sql,params=None):
        super().execute(sql,params)
        q=' '.join(sql.split()).lower()
        if q.startswith('select venue_id from portfolio_v2.venues where venue_id=%s'):
            self.current=[(self.venue,)] if params[0]==self.venue else []
        elif q.startswith('select wine_id,source_metadata,source_characteristics'):
            self.current=[(wine_id, w['source_metadata'],w['source_characteristics'],
                           w['source_body'],w['source_pairings'],w['suggested_pairings'],
                           w['pairings_reviewed'],w['pairings']) for wine_id,w in self.live.items()]
        elif q.startswith('select wine_id,wine_name,pronunciation,characteristics,pairings'):
            self.current=[(wine_id,w['wine_name'],w['pronunciation'],w['characteristics'],
                           w['pairings'],w['pairings_reviewed']) for wine_id,w in self.live.items()]
        elif q.startswith('insert into portfolio_v2.wine_review_archive'):
            # Archive INSERT ... SELECT must be scoped to one venue, atomic with DELETE.
            assert 'from portfolio_v2.wines where venue_id=%s' in q and 'on conflict' in q
            assert params==(self.venue,)
            for wine_id,w in self.live.items():self.archived[wine_id]=w.copy()
        elif q.startswith('select wine_id,pronunciation,characteristics,pairings') and 'from portfolio_v2.wine_review_archive' in q:
            assert params==(self.venue,MAX_CATALOG_ROWS+1)
            self.current=[(wine_id,w['pronunciation'],w['characteristics'],w['pairings'],
                           w['pairings_reviewed'],w['source_metadata'],w['source_characteristics'],
                           w['source_body'],w['source_pairings'],w['suggested_pairings'])
                          for wine_id,w in self.archived.items()]
        elif q.startswith('delete from portfolio_v2.wines'):
            assert params==(self.venue,)
            self.live={}
        elif q.startswith('insert into portfolio_v2.wines'):
            self.inserted.append(params)
            self.live[params[1]]={'wine_name':params[2], 'pronunciation':params[3],
                        'characteristics':json.loads(params[8]),'pairings':json.loads(params[9]),
                        'pairings_reviewed':params[17],'source_metadata':params[12],
                        'source_characteristics':params[13],'source_body':params[14],
                        'source_pairings':params[15], 'suggested_pairings':json.loads(params[16])}


class V12IndependentSix(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root=Path(temp.name)
        self.wine=next(w for w in load_catalog() if w.restaurant_id=='venue_a')

    def test_p11_01_postmark_mismatched_recipient_not_accepted(self):
        for field in ('different@example.test',None,'a@example.test, b@example.test','',False,'Recipient <dest@example.test>'):
            with self.subTest(returned=field):
                payload={'ErrorCode':0,'MessageID':'synthetic-123'}
                if field is not None:payload['To']=field
                sender=PostmarkSender('dummy','sender@example.test',
                    opener=lambda req,timeout:MockResponse(payload))
                with self.assertRaises(SubmissionUncertain):
                    sender.send('dest@example.test',[])

    def test_p11_01_postmark_exactly_matching_returned_to_passes(self):
        sender=PostmarkSender('dummy','sender@example.test',
                    opener=lambda req,timeout:MockResponse({
                        'ErrorCode':0,'MessageID':'synthetic-123','To':'dest@EXAMPLE.TEST'}))
        self.assertEqual(sender.send('dest@example.test',[]),'synthetic-123')
        for field in ('Cc','Bcc'):
            sender2=PostmarkSender('dummy','sender@example.test',
                opener=lambda req,timeout,kind=field:MockResponse({
                  'ErrorCode':0,'MessageID':'synthetic-123','To':'dest@example.test',
                  kind:'other@example.test'}))
            with self.subTest(field=field),self.assertRaises(SubmissionUncertain):
                sender2.send('dest@example.test',[])

    def test_p11_02_null_nonstring_and_whitespace_legacy_names_rejected(self):
        valid=(1,'Example Merlot',True,False,'Origin','red','merlot','body','full','dry','Pairs with steak',None)
        for bad in (None,' ',42,False,b'Example', 'z'*1501):
            with self.subTest(bad=repr(bad)):
                row=list(valid);row[1]=bad
                with self.assertRaises(CatalogDataError):legacy_row_to_wine(tuple(row))
        self.assertEqual(legacy_row_to_wine(valid).wine_name,'Example Merlot')

    def test_p11_03_additive_migration_checks_historical_suggestions(self):
        old=(ROOT/'examples/migrations/005_canonical_tags.sql').read_text()
        new=(ROOT/'examples/migrations/006_review_archive_and_suggestions.sql').read_text()
        self.assertNotIn('valid_tag_array(suggested_pairings)',old)
        for col in ('characteristics','pairings','suggested_pairings'):
            self.assertIn('NOT portfolio_v2.valid_tag_array('+col+')',new)
        self.assertIn('RAISE EXCEPTION',new)
        self.assertIn('wine_review_archive',new)
        self.assertIn('INSERT INTO portfolio_v2.wine_review_archive',new)

    def test_p11_04_reviewed_enrichment_survives_zero_inventory_reactivation(self):
        pg=StatefulReviewPG(self.wine)
        store=PostgresCatalogStore(lambda:pg)
        self.assertEqual(store.replace_venue('venue_a',[],allow_empty=True),0)
        self.assertEqual(pg.live,{})
        self.assertEqual(pg.archived[self.wine.wine_id]['pairings'],['red_meat'])
        self.assertEqual(store.replace_venue('venue_a',[replace(self.wine,pairings=frozenset())]),1)
        restored=pg.live[self.wine.wine_id]
        self.assertEqual(restored['pairings'],['red_meat'])
        self.assertTrue(restored['pairings_reviewed'])
        self.assertEqual(restored['source_metadata'],'original source metadata')
        self.assertEqual(restored['source_pairings'],'original source dish prose')
        self.assertEqual(restored['pronunciation'],'Reviewed Pronunciation')

    def test_p11_04_review_archive_is_scoped_to_venue_and_transaction(self):
        pg=StatefulReviewPG(self.wine)
        store=PostgresCatalogStore(lambda:pg)
        store.replace_venue('venue_a',[],allow_empty=True)
        q=[query for query,_ in pg.queries]
        archive_idx=next(i for i,s in enumerate(q) if s.startswith('insert into portfolio_v2.wine_review_archive'))
        delete_idx=next(i for i,s in enumerate(q) if s.startswith('delete from portfolio_v2.wines'))
        self.assertLess(archive_idx,delete_idx)
        self.assertEqual(pg.archived.keys(),{self.wine.wine_id})
        self.assertTrue(pg.closed)

    def test_p11_04_legacy_import_restores_pairings_after_empty_snapshot(self):
        legacy=(1,'Example Merlot',True,False,'Fictional Origin','red','merlot','body','full','dry',
                'Pairs with steak','new legacy metadata')
        from postgres_catalog import legacy_row_to_wine
        w=legacy_row_to_wine(legacy)
        pg=StatefulReviewPG(w,legacy=True)
        pg.legacy_rows=()
        store=PostgresCatalogStore(lambda:pg)
        self.assertEqual(store.import_legacy(),{'legacy-1':0})
        self.assertEqual(pg.live,{})
        pg.legacy_rows=(legacy,)
        self.assertEqual(store.import_legacy(),{'legacy-1':1})
        restored=pg.live[w.wine_id]
        self.assertEqual(restored['pairings'],['red_meat'])
        self.assertTrue(restored['pairings_reviewed'])
        self.assertEqual(restored['source_metadata'],'new legacy metadata')

    def test_p11_05_schema_rejects_unvalidated_constraints(self):
        # Build the full required column map to reach the constraint checks.
        columns=[(table,col,kind,'YES' if table in ('wines','wine_review_archive')
             and col in NULLABLE_WINE_COLUMNS else 'NO')
             for table,spec in REQUIRED_SCHEMA_COLUMNS.items() for col,kind in spec.items()]
        with self.assertRaisesRegex(RuntimeError,'validation metadata'):
            _validate_schema_metadata(columns, [('wines','x','c','CHECK(TRUE)')],[])
        with self.assertRaisesRegex(RuntimeError,'unvalidated'):
            _validate_schema_metadata(columns, [('wines','x','c','CHECK(TRUE)',False,(),None,())],[])
        with self.assertRaisesRegex(RuntimeError,'unenforced'):
            _validate_schema_metadata(columns, [('wines','x','c','CHECK(TRUE) NOT ENFORCED',True,(),None,())],[])
        src=(ROOT/'examples/postgres_catalog.py').read_text()
        self.assertIn('con.convalidated',src)

    def test_p11_05_required_archive_schema_and_foreign_key(self):
        self.assertIn('wine_review_archive',REQUIRED_SCHEMA_COLUMNS)
        migration=(ROOT/'examples/migrations/006_review_archive_and_suggestions.sql').read_text()
        self.assertIn('REFERENCES portfolio_v2.venues',migration)
        self.assertIn('valid_tag_array(suggested_pairings)',migration)

    def test_p11_06_sqlite_read_refuses_oversized_direct_sql_catalog(self):
        store=CatalogStore(self.root/'oversized.sqlite3')
        self.addCleanup(store.close)
        with store._connect() as conn:
            conn.execute('INSERT INTO restaurants(restaurant_id) VALUES (?)',('venue_a',))
            conn.executemany('''INSERT INTO catalog(restaurant_id,wine_id,wine_name,pronunciation,
              origin,category,type,sweetness,characteristics,pairings,available_by_glass,
              available_by_bottle) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',
              (('venue_a',f'row-{i}', 'Fictional Wine','Fictional Pronunciation','Imaginary',
                'red','merlot','dry','[]','[]',1,0)
                for i in range(MAX_CATALOG_ROWS+1)))
        with self.assertRaisesRegex(CatalogDataError,'exceeds row limit'):
            store.wines_for_venue('venue_a')
        self.assertEqual(store.wines_for_venue('unknown'),())

    def test_p11_06_postgres_reader_fails_closed_at_max_plus_one(self):
        class OverlargePG(FakePG):
            def execute(self,sql,params=None):
                super().execute(sql,params)
                if 'from portfolio_v2.wines where venue_id=%s order by wine_id' in sql.lower():
                    self.current=[()]*(MAX_CATALOG_ROWS+1)
        pg=OverlargePG()
        with self.assertRaisesRegex(CatalogDataError,'exceeds row limit'):
            PostgresCatalogStore(lambda:pg).wines_for_venue('venue_a')
        read=[(q,p) for q,p in pg.queries if 'order by wine_id' in q]
        self.assertEqual(len(read),1)
        self.assertIn('limit %s',read[0][0])
        self.assertEqual(read[0][1],('venue_a',MAX_CATALOG_ROWS+1))


if __name__=='__main__':unittest.main()
