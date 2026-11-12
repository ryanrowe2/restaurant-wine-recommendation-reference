"""Adversarial v10 fixes, using only fictitious values and throwaway storage.

V9-01: ZIP metadata/trailing-byte smuggling, including the entire release gate.
V9-02: explicitly authorized, atomic empty catalog replacement.
V9-03: direct-SQL writes cannot evade required text-field constraints.
V9-04: Postmark zero error code has an exact integer type contract.
"""
from __future__ import annotations
import io
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from dataclasses import replace

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'examples'),str(ROOT/'scripts')]
from catalog_store import CatalogStore
from pipeline import load_catalog
from postgres_catalog import PostgresCatalogStore
from providers import PostmarkSender
from delivery import SubmissionUncertain,PermanentProviderReject
from release_gate import scan_distribution
from acceptance_gate import verify
from test_v3_postgres_contract import FakePG

BASE='restaurant-wine-recommendation-reference'

class Response:
    status=200
    def __init__(self,payload):self.content=io.BytesIO(json.dumps(payload).encode())
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def read(self,n):return self.content.read(n)

class V10FourFreshFixes(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.tmp=Path(self.temp.name)
        self.zip=self.tmp/'probe.zip'
        self.wines=[w for w in load_catalog() if w.restaurant_id=='venue_a']

    def _small_zip(self):
        with zipfile.ZipFile(self.zip,'w',compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(BASE+'/README.md','Fictional safe content')
        return self.zip

    def test_v9_01_zip_comment_fails_scanner_and_gate(self):
        self._small_zip()
        synthetic='github'+'_pat_'+'A'*37
        with zipfile.ZipFile(self.zip,'a') as archive:
            archive.comment=('hidden '+synthetic).encode()
        self.assertIsNone(zipfile.ZipFile(self.zip).testzip())
        errors=scan_distribution(self.zip)
        self.assertTrue(any('ZIP comment' in e for e in errors),errors)
        result=verify(self.zip)
        self.assertFalse(result['offline_gate_passed'])
        self.assertEqual(result['decision'],'BLOCKED: unsafe artifact')

    def test_v9_01_trailing_bytes_fails_scanner_and_gate(self):
        self._small_zip()
        with self.zip.open('ab') as fp:fp.write(('github'+'_pat_'+'B'*37).encode())
        self.assertIsNone(zipfile.ZipFile(self.zip).testzip())
        self.assertTrue(scan_distribution(self.zip))
        self.assertEqual(verify(self.zip)['decision'],'BLOCKED: unsafe artifact')

    def test_v9_01_member_comments_and_extras_rejected(self):
        for variant in ('comment','extra'):
            with self.subTest(variant=variant):
                member=zipfile.ZipInfo(BASE+'/README.md')
                if variant=='comment':member.comment=b'not scanned text'
                else:member.extra=b'AB\x04\x00junk'
                with zipfile.ZipFile(self.zip,'w') as z:
                    z.writestr(member,'safe')
                self.assertTrue(scan_distribution(self.zip))

    def test_v9_01_narrow_git_commit_comment_remains_compatible(self):
        self._small_zip()
        with zipfile.ZipFile(self.zip,'a') as archive:archive.comment=b'a'*40
        self.assertTrue(scan_distribution(self.zip))
        self.assertEqual(scan_distribution(self.zip,expected_git_commit='a'*40),[])
        # Git archive also uses a benign numeric UT extra field.
        member=zipfile.ZipInfo(BASE+'/README.md')
        member.extra=b'UT\x05\x00\x01\x00\x00\x00\x00'
        with zipfile.ZipFile(self.zip,'w') as z:
            z.comment=b'a'*40
            z.writestr(member,'safe')
        self.assertTrue(scan_distribution(self.zip))
        self.assertEqual(scan_distribution(self.zip,expected_git_commit='a'*40),[])

    def test_v9_02_sqlite_empty_authorized_snapshot_is_atomic_and_scoped(self):
        store=CatalogStore(self.tmp/'catalog.sqlite3')
        self.addCleanup(store.close)
        store.replace_venue('venue_a',self.wines)
        store.replace_venue('venue_b',[w for w in load_catalog() if w.restaurant_id=='venue_b'])
        with self.assertRaises(ValueError):store.replace_venue('venue_a',[])
        with self.assertRaises(ValueError):store.replace_venue('unknown',[],allow_empty=True)
        self.assertEqual(store.replace_venue('venue_a',[],allow_empty=True),0)
        self.assertTrue(store.has_venue('venue_a'))
        self.assertEqual(store.wines_for_venue('venue_a'),())
        self.assertTrue(store.wines_for_venue('venue_b'))
        self.assertEqual(store.replace_venue('venue_a',self.wines),len(self.wines))

    def test_v9_02_postgres_empty_authorized_snapshot_is_scoped(self):
        class RegisteredPG(FakePG):
            def execute(self,sql,params=None):
                super().execute(sql,params)
                if sql.strip().lower().startswith('select venue_id from portfolio_v2.venues'):
                    self.current=[('venue_a',)] if params==('venue_a',) else []
        pg=RegisteredPG(provenance=((self.wines[0].wine_id,None,None,None,None,[],False,[]),))
        store=PostgresCatalogStore(lambda:pg)
        with self.assertRaises(ValueError):store.replace_venue('venue_a',[])
        self.assertEqual(store.replace_venue('venue_a',[],allow_empty=True),0)
        deletes=[params for sql,params in pg.queries if sql.startswith('delete from portfolio_v2.wines')]
        self.assertEqual(deletes,[('venue_a',)])
        self.assertFalse(any(sql.startswith('insert into portfolio_v2.wines') for sql,_ in pg.queries))
        self.assertTrue(pg.closed)
        unknown=RegisteredPG()
        with self.assertRaises(ValueError):PostgresCatalogStore(lambda:unknown).replace_venue('unknown',[],allow_empty=True)
        self.assertFalse(any(sql.startswith('delete from portfolio_v2.wines') for sql,_ in unknown.queries))

    def test_v9_03_sqlite_rejects_blank_or_oversized_required_fields(self):
        store=CatalogStore(self.tmp/'catalog.sqlite3')
        self.addCleanup(store.close)
        store.replace_venue('venue_a',self.wines)
        for field in ('wine_id','wine_name','pronunciation','origin','type'):
            for bad in ('', '   ', 'x'*1501):
                with self.subTest(field=field,bad_length=len(bad)):
                    with self.assertRaises(sqlite3.IntegrityError),store._connect() as conn:
                        conn.execute(f'UPDATE catalog SET {field}=? WHERE restaurant_id=? AND wine_id=?',
                                     (bad,'venue_a',self.wines[0].wine_id))
        self.assertEqual(len(store.wines_for_venue('venue_a')),len(self.wines))

    def test_v9_03_sqlite_direct_insert_cannot_persist_empty_name(self):
        store=CatalogStore(self.tmp/'catalog.sqlite3')
        self.addCleanup(store.close)
        store.replace_venue('venue_a',self.wines)
        w=self.wines[0]
        with self.assertRaises(sqlite3.IntegrityError),store._connect() as conn:
            conn.execute('''INSERT INTO catalog(restaurant_id,wine_id,wine_name,pronunciation,origin,category,type,
               sweetness,characteristics,pairings,available_by_glass,available_by_bottle)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',
               (w.restaurant_id,'malformed',' ',w.pronunciation,w.origin,w.category,w.type,w.sweetness,
                '[]','[]',1,0))

    def test_v9_03_postgres_migration_enforces_required_text(self):
        migrations=(ROOT/'examples/migrations')
        source=(migrations/'004_required_wine_fields.sql').read_text()
        for field in ('wine_id','wine_name','pronunciation','origin','wine_type'):
            self.assertIn('btrim('+field+')',source)
        self.assertIn('wine_required_text_valid',source)
        self.assertIn('004_required_wine_fields.sql',(ROOT/'scripts/release_gate.py').read_text())

    def test_v9_04_postmark_acceptance_requires_exact_integer_code(self):
        for malformed in (False,0.0,None,'0',[],{},True):
            sender=PostmarkSender('fictional-token','sender@example.test',opener=lambda req,timeout,value=malformed:Response({'ErrorCode':value,'MessageID':'fake-ref'}))
            with self.subTest(code=malformed),self.assertRaises(SubmissionUncertain):
                sender.send('recipient@example.test',[])
        accepted=PostmarkSender('fictional-token','sender@example.test',opener=lambda r,timeout:Response({'ErrorCode':0,'MessageID':'real-shaped-test-id','To':'recipient@example.test'}))
        self.assertEqual(accepted.send('recipient@example.test',[]),'real-shaped-test-id')
        rejected=PostmarkSender('fictional-token','sender@example.test',opener=lambda r,timeout:Response({'ErrorCode':422,'MessageID':'unused'}))
        with self.assertRaises(PermanentProviderReject):rejected.send('recipient@example.test',[])

if __name__=='__main__':unittest.main()
