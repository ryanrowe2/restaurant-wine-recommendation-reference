"""Regressions for v8 independent audit findings V8-A01 through V8-A04.

All archive token fixtures are constructed at runtime and are synthetic.
No real credentials, restaurant data, or live services are needed.
"""
from __future__ import annotations
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
sys.path.insert(0,str(ROOT/'examples'))
from release_gate import scan_distribution
from acceptance_gate import verify
from pipeline import CatalogDataError
from postgres_catalog import PostgresCatalogStore
from test_v3_postgres_contract import FakePG

ROOT_NAME='restaurant-wine-recommendation-reference'

class V9IndependentRepairs(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.archive=Path(self.temp.name)/'probe.zip'

    def _zip(self, entries):
        with zipfile.ZipFile(self.archive,'w',compression=zipfile.ZIP_DEFLATED) as zf:
            for path,contents in entries:
                zf.writestr(path,contents)
        return self.archive

    def test_v8_a01_parent_traversal_fails_scanner_and_full_gate(self):
        self._zip([(f'{ROOT_NAME}/README.md','harmless'),
                   (f'{ROOT_NAME}/../unscanned.py','ordinary synthetic payload')])
        scan=scan_distribution(self.archive)
        self.assertTrue(any('Unsafe or noncanonical' in x for x in scan),scan)
        result=verify(self.archive)
        self.assertFalse(result['offline_gate_passed'])
        self.assertEqual(result['decision'],'BLOCKED: unsafe artifact')

    def test_v8_a01_multiple_escapes_and_noncanonical_directories_rejected(self):
        for pathname in (f'{ROOT_NAME}/../../escape.txt',
                         f'{ROOT_NAME}/../concealed/',
                         f'{ROOT_NAME}/./../fake.txt'):
            with self.subTest(pathname=pathname):
                self._zip([(f'{ROOT_NAME}/README.md','harmless'),(pathname,'fake test data')])
                self.assertTrue(scan_distribution(self.archive))

    def test_v8_a02_fine_grained_token_fails_scanner_and_full_gate(self):
        token='github'+'_pat_'+'A'*35 # synthetic; runtime construction avoids false scanning of source fixture
        self._zip([(f'{ROOT_NAME}/README.md','safe'),
                   (f'{ROOT_NAME}/docs/example.md',f'Testing token {token}\n')])
        scan=scan_distribution(self.archive)
        self.assertTrue(any('possible credential' in x for x in scan),scan)
        self.assertEqual(verify(self.archive)['decision'],'BLOCKED: unsafe artifact')

    def test_v8_a02_assignment_variants_cannot_bypass_scan(self):
        assignment='GITHUB'+'_TOKEN='+'B'*35
        self._zip([(f'{ROOT_NAME}/README.md','safe'),(f'{ROOT_NAME}/docs/test.md',assignment)])
        self.assertTrue(any('suspicious credential assignment' in x for x in scan_distribution(self.archive)))

    @staticmethod
    def _source_row(venue=1):
        return (venue,'Example Cabernet',True,False,'Fictional origin','red','cabernet',
                'fruit','full','dry','Pairs with steak',None)

    def test_v8_a03_authoritative_snapshot_clears_empty_registry_venue(self):
        pg=FakePG(registry_ids=(1,2),legacy_rows=(self._source_row(1),))
        result=PostgresCatalogStore(lambda:pg).import_legacy()
        self.assertEqual(result,{'legacy-1':1,'legacy-2':0})
        deletions=[params for sql,params in pg.queries if sql.startswith('delete from portfolio_v2.wines')]
        self.assertEqual(deletions,[('legacy-1',),('legacy-2',)])
        inserted=[params for sql,params in pg.queries if sql.startswith('insert into portfolio_v2.wines')]
        self.assertEqual(len(inserted),1)
        self.assertEqual(inserted[0][0],'legacy-1')
        self.assertNotIn(('fictional-unrelated',),deletions)
        self.assertTrue(pg.closed)

    def test_v8_a03_all_registered_venues_can_have_empty_inventory(self):
        pg=FakePG(registry_ids=(1,2),legacy_rows=())
        self.assertEqual(PostgresCatalogStore(lambda:pg).import_legacy(),{'legacy-1':0,'legacy-2':0})
        deletes=[params for sql,params in pg.queries if sql.startswith('delete from portfolio_v2.wines')]
        self.assertEqual(deletes,[('legacy-1',),('legacy-2',)])
        self.assertFalse(any(sql.startswith('insert into portfolio_v2.wines') for sql,_ in pg.queries))

    def test_v8_a03_reject_orphan_wine_and_preserve_state(self):
        pg=FakePG(registry_ids=(2,),legacy_rows=(self._source_row(1),))
        with self.assertRaisesRegex(CatalogDataError,'trusted source registry'):
            PostgresCatalogStore(lambda:pg).import_legacy()
        self.assertFalse(any(sql.startswith('delete from portfolio_v2.wines') for sql,_ in pg.queries))
        self.assertTrue(pg.closed)

    def test_v8_a03_failure_rolls_back_atomic_batch(self):
        class FailingPG(FakePG):
            def __exit__(self, exc_type, exc, tb):
                self.rolled_back=exc_type is not None
                return False
            def execute(self,query,params=None):
                if query.strip().lower().startswith('delete from portfolio_v2.wines') and params==('legacy-2',):
                    raise RuntimeError('simulated DB failure during second venue replacement')
                return super().execute(query,params)
        pg=FailingPG(registry_ids=(1,2),legacy_rows=(self._source_row(1),))
        with self.assertRaisesRegex(RuntimeError,'simulated DB failure'):
            PostgresCatalogStore(lambda:pg).import_legacy()
        self.assertTrue(pg.rolled_back)
        self.assertTrue(pg.closed)

    def test_v8_a04_current_verification_document_does_not_claim_v3_is_current(self):
        doc=(ROOT/'docs/verification-and-tradeoffs.md').read_text(encoding='utf-8')
        self.assertNotIn('current v3 remediation status',doc)
        self.assertIn('current release acceptance policy',doc)
        example=(ROOT/'examples/README.md').read_text(encoding='utf-8')
        self.assertNotIn('Latest v5 source hardening',example)

if __name__=='__main__':unittest.main()
