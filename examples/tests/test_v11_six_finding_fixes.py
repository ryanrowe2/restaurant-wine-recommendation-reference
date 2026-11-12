"""Independent v11 repair regressions using synthetic catalogs and mocked providers.

PP10-01..06: persisted SQLite type guards, legacy registry retirement,
sole-recipient receipts, authenticated ZIP comment provenance, canonical
PostgreSQL tags, and current release documentation.
"""
from __future__ import annotations
import hashlib
import io
import json
import sqlite3
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'examples'),str(ROOT/'scripts')]
from catalog_store import CatalogStore
from pipeline import load_catalog
from postgres_catalog import PostgresCatalogStore
from providers import PostmarkReceiptVerifier
from release_gate import scan_distribution
from acceptance_gate import verify
from test_v3_postgres_contract import FakePG

class Response:
    status=200
    def __init__(self,details):self.body=io.BytesIO(json.dumps(details).encode())
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def read(self,n):return self.body.read(n)

class V11SixFindingFixes(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.tmp=Path(temp.name)
        self.wines=[w for w in load_catalog() if w.restaurant_id=='venue_a']

    def test_pp10_01_sqlite_rejects_binary_required_fields_on_direct_update(self):
        store=CatalogStore(self.tmp/'wine.sqlite3')
        self.addCleanup(store.close)
        store.replace_venue('venue_a',self.wines)
        wine=self.wines[0]
        for field in ('restaurant_id','wine_id','wine_name','pronunciation','origin','type'):
            with self.subTest(field=field),store._connect() as conn:
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(f'UPDATE catalog SET {field}=? WHERE restaurant_id=? AND wine_id=?',
                                 (b'Binary Wine Value','venue_a',wine.wine_id))
        self.assertEqual(len(store.wines_for_venue('venue_a')),len(self.wines))

    def test_pp10_01_sqlite_rejects_binary_required_fields_on_direct_insert(self):
        store=CatalogStore(self.tmp/'wine.sqlite3')
        self.addCleanup(store.close)
        store.replace_venue('venue_a',self.wines)
        existing=self.wines[0]
        params=('venue_a','synthetic-new',existing.wine_name,existing.pronunciation,existing.origin,
                existing.category,existing.type,existing.sweetness,'[]','[]',1,0)
        for index in (0,1,2,3,4,6):
            with self.subTest(index=index),store._connect() as conn:
                malformed=list(params);malformed[index]=b'Binary Wine Value'
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute('''INSERT INTO catalog(restaurant_id,wine_id,wine_name,pronunciation,origin,
                        category,type,sweetness,characteristics,pairings,available_by_glass,
                        available_by_bottle) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',malformed)

    def test_pp10_02_removed_legacy_venue_deletes_only_its_inventory(self):
        class StagedPG(FakePG):
            def execute(self,query,params=None):
                super().execute(query,params)
                if 'from portfolio_v2.venues' in query.lower():
                    self.current=[('legacy-1',),('legacy-2',)]
        pg=StagedPG(registry_ids=(1,),legacy_rows=())
        self.assertEqual(PostgresCatalogStore(lambda:pg).import_legacy(),{'legacy-1':0})
        deleted=[p[0] for q,p in pg.queries if q.startswith('delete from portfolio_v2.wines')]
        self.assertEqual(deleted,['legacy-2','legacy-1'])
        self.assertFalse(any(q.startswith('delete from portfolio_v2.venues') or
                             q.startswith('delete from portfolio_v2.legacy_identity_map') for q,p in pg.queries))
        self.assertTrue(pg.closed)

    def test_pp10_02_staged_registry_rejects_unexpected_curated_venue(self):
        from pipeline import CatalogDataError
        class MalformedPG(FakePG):
            def execute(self,query,params=None):
                super().execute(query,params)
                if 'from portfolio_v2.venues' in query.lower():
                    self.current=[('venue_curated',)]
        with self.assertRaisesRegex(CatalogDataError,'non-legacy identity'):
            PostgresCatalogStore(lambda:MalformedPG(registry_ids=(),legacy_rows=())).import_legacy()

    def test_pp10_02_empty_source_retires_all_staged_legacy_wine_inventories(self):
        class StagedPG(FakePG):
            def execute(self,query,params=None):
                super().execute(query,params)
                if 'from portfolio_v2.venues' in query.lower():
                    self.current=[('legacy-1',),('legacy-2',)]
        pg=StagedPG(registry_ids=(),legacy_rows=())
        self.assertEqual(PostgresCatalogStore(lambda:pg).import_legacy(),{})
        self.assertEqual([p[0] for q,p in pg.queries if q.startswith('delete from portfolio_v2.wines')],
                         ['legacy-1','legacy-2'])

    def test_pp10_02_bounded_staged_legacy_registry(self):
        from pipeline import CatalogDataError,MAX_CATALOG_ROWS
        class Oversized(FakePG):
            def execute(self,query,params=None):
                super().execute(query,params)
                if 'from portfolio_v2.venues' in query.lower():
                    self.current=[('legacy-'+str(i),) for i in range(MAX_CATALOG_ROWS+1)]
        pg=Oversized(registry_ids=(),legacy_rows=())
        with self.assertRaisesRegex(CatalogDataError,'staged legacy venue count'):
            PostgresCatalogStore(lambda:pg).import_legacy()

    def test_pp10_03_postmark_receipt_exactly_one_recipient_no_cc_bcc(self):
        ref='fake-message-id';digest=hashlib.sha256(b'fictional-event').hexdigest()
        details={'MessageID':ref,'Status':'Sent','From':'Sender <sender@example.test>',
                 'Recipients':['diner@example.test'],'To':'diner@example.test',
                 'Metadata':{'PalateEvent':digest}}
        verify_receipt=PostmarkReceiptVerifier('fictional-token','sender@example.test',
                                               opener=lambda req,timeout:Response(details))
        self.assertTrue(verify_receipt(ref,'diner@example.test',digest))
        for field,value in (('Recipients',['diner@example.test','extra@example.test']),
                            ('Recipients',['diner@example.test','diner@example.test']),
                            ('To','diner@example.test,extra@example.test'),
                            ('Cc','extra@example.test'),('Bcc','extra@example.test'),
                            ('To','someone-else@example.test')):
            with self.subTest(field=field,value=value):
                bad=dict(details);bad[field]=value
                details.clear();details.update(bad)
                self.assertFalse(verify_receipt(ref,'diner@example.test',digest))
                details.clear();details.update({'MessageID':ref,'Status':'Sent','From':'Sender <sender@example.test>',
                       'Recipients':['diner@example.test'],'To':'diner@example.test','Metadata':{'PalateEvent':digest}})

    def test_pp10_03_postmark_recipient_dict_and_string_variants(self):
        ref='test-ref';digest='e'*64
        details={'MessageID':ref,'Status':'Sent','From':'sender@example.test',
                 'Recipients':[{'Email':'diner@example.test'}],'Metadata':{'PalateEvent':digest}}
        verifier=PostmarkReceiptVerifier('mock','sender@example.test',opener=lambda r,timeout:Response(details))
        self.assertTrue(verifier(ref,'diner@example.test',digest))
        details['Recipients']='diner@example.test'
        self.assertTrue(verifier(ref,'diner@example.test',digest))
        details['Recipients']='diner@example.test, extra@example.test'
        self.assertFalse(verifier(ref,'diner@example.test',digest))

    def test_pp10_04_hex_comment_requires_externally_pinned_git_commit(self):
        archive=self.tmp/'comment.zip'
        with zipfile.ZipFile(archive,'w',compression=zipfile.ZIP_DEFLATED) as z:
            z.comment=b'a'*40
            z.writestr('restaurant-wine-recommendation-reference/README.md','synthetic')
        self.assertTrue(scan_distribution(archive))
        self.assertEqual(scan_distribution(archive,expected_git_commit='a'*40),[])
        self.assertTrue(scan_distribution(archive,expected_git_commit='b'*40))
        self.assertFalse(verify(archive)['offline_gate_passed'])
        self.assertFalse(verify(archive,expected_git_commit='b'*40)['offline_gate_passed'])
        with zipfile.ZipFile(archive,'a') as z:z.comment=b'fake-metadata'
        self.assertTrue(scan_distribution(archive,expected_git_commit='a'*40))

    def test_pp10_04_ci_supplies_authenticated_git_identity(self):
        workflow=(ROOT/'.github/workflows/verify.yml').read_text('utf-8')
        self.assertIn('--expected-git-commit "$(git rev-parse HEAD)"',workflow)
        self.assertIn('expected_git_commit=expected_git_commit',(ROOT/'scripts/acceptance_gate.py').read_text())

    def test_pp10_05_canonical_postgres_tags_have_additive_guarded_migration(self):
        mig=(ROOT/'examples/migrations/005_canonical_tags.sql').read_text()
        self.assertIn("'^[a-z0-9_-]{1,48}$'",mig)
        self.assertIn('RAISE EXCEPTION',mig)
        self.assertIn('portfolio_v2.valid_tag_array',mig)
        source=(ROOT/'examples/postgres_catalog.py').read_text()
        for specimen in ('[" "]','["a b"]','["red_meat"]'):
            self.assertIn(specimen,source)
        self.assertIn('005_canonical_tags.sql',(ROOT/'scripts/release_gate.py').read_text())

    def test_pp10_06_documentation_identifies_previous_versions_as_historical(self):
        docs=(ROOT/'docs/verification-and-tradeoffs.md').read_text()
        self.assertIn('Historical v9 release hardening',docs)
        self.assertIn('v11-six-finding-remediation.md',docs)
        self.assertNotIn('Current v9 release hardening',docs)

if __name__=='__main__':unittest.main()
