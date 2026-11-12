"""PostgreSQL SQL-generation and provenance regression contracts, mocked connection.

These do not replace real PostgreSQL integration tests, which require a staging DB.
"""
from __future__ import annotations
import json
import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from postgres_catalog import PostgresCatalogStore, legacy_row_to_wine, extract_reviewable_pairings
from pipeline import Wine, CatalogDataError, load_catalog

class FakePG:
    def __init__(self, *, legacy_rows=(),aliases=(),old=(),provenance=(),suggested=('red_meat',),registry_ids=None):
        self.legacy_rows=tuple(legacy_rows);self.registry_ids=tuple(sorted({row[0] for row in legacy_rows}) if registry_ids is None else registry_ids);self.aliases=list(aliases)
        self.old=tuple(old);self.provenance=tuple(provenance)
        self.suggested=tuple(suggested)
        self.queries=[];self.current=[];self.closed=False
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def close(self):self.closed=True
    def cursor(self,*args,**kwargs):
        self.cursor_calls=getattr(self,'cursor_calls',[])+[kwargs]
        return self
    def execute(self,query,params=None):
        sql=' '.join(query.split()).lower()
        self.queries.append((sql,params))
        self.current=[]
        if 'from public.restaurants' in sql:self.current=[(i,) for i in self.registry_ids]
        elif 'from public.wines' in sql:self.current=list(self.legacy_rows)
        elif 'select legacy_name,wine_id from portfolio_v2.legacy_identity_map' in sql:self.current=list(self.aliases)
        elif 'select wine_id,wine_name,pronunciation,characteristics,pairings' in sql:self.current=list(self.old)
        elif 'select wine_id,source_metadata,source_characteristics' in sql:self.current=list(self.provenance)
        elif 'select suggested_pairings from portfolio_v2.wines' in sql:self.current=[(list(self.suggested),)]
        elif 'select 1 from portfolio_v2.wines' in sql:self.current=[(1,)]
        elif 'select wine_id from portfolio_v2.legacy_identity_map' in sql:
            self.current=[(p[2],) for q,p in self.queries if q.startswith('insert into portfolio_v2.legacy_identity_map')][-1:] or [(self.aliases[0][1],)]
    def fetchall(self):return list(self.current)
    def fetchmany(self,size):return list(self.current[:size])
    def fetchone(self):return self.current[0] if self.current else None

class PgContracts(unittest.TestCase):
    def test_06_replace_preserves_source_and_review_state(self):
        w=next(w for w in load_catalog() if w.restaurant_id=='venue_a')
        pg=FakePG(provenance=((w.wine_id,'original tags','original characteristics','original body',
                              'original pairings',['red_meat'],False,[]),))
        self.assertEqual(PostgresCatalogStore(lambda:pg).replace_venue('venue_a',[w]),1)
        insert=[p for q,p in pg.queries if q.startswith('insert into portfolio_v2.wines')]
        self.assertEqual(len(insert),1)
        params=insert[0]
        self.assertEqual(params[12:16],('original tags','original characteristics','original body','original pairings'))
        self.assertEqual(json.loads(params[9]),[])  # Inference has not been approved.
        self.assertEqual(json.loads(params[16]),['red_meat'])
        self.assertFalse(params[17])
        self.assertTrue(pg.closed)
    def test_05_new_import_stages_only_reviewable_suggestions(self):
        row=(1,'Example Cabernet',True,False,'Origin','red','cabernet','fruit','full','dry','Pairs with steak', 'pairs_with_red_meat')
        pg=FakePG(legacy_rows=(row,))
        result=PostgresCatalogStore(lambda:pg).import_legacy()
        self.assertEqual(result,{'legacy-1':1})
        inserted=[p for q,p in pg.queries if q.startswith('insert into portfolio_v2.wines')]
        self.assertEqual(len(inserted),1)
        params=inserted[0]
        self.assertEqual(json.loads(params[9]),[])
        self.assertEqual(json.loads(params[16]),['red_meat'])
        self.assertEqual(json.loads(params[8]),[])  # Historical arbitrary metadata not approved.
        self.assertFalse(params[17])
        self.assertEqual(params[15],'Pairs with steak')
    def test_05_negation_is_never_auto_suggested(self):
        for source in ('Not suitable with steak','Avoid spicy food entirely','Without chicken','Do not pair with salmon'):
            with self.subTest(source=source):self.assertFalse(extract_reviewable_pairings(source))
    def test_15_renamed_alias_preserves_stable_id_and_enrichment(self):
        row=(1,'New Label',True,False,'Origin','red','cabernet','fruit','full','dry','Pairs with steak', None)
        prior_id='stable-id-123'
        pg=FakePG(legacy_rows=(row,),aliases=(('new label',prior_id),),
                  old=((prior_id,'Old Label','Reviewed Pronunciation',['fruity'],['fish'],True),))
        PostgresCatalogStore(lambda:pg).import_legacy()
        params=[p for q,p in pg.queries if q.startswith('insert into portfolio_v2.wines')][0]
        self.assertEqual(params[1],prior_id)
        self.assertEqual(params[3],'Reviewed Pronunciation')
        self.assertIn('fruity',json.loads(params[8]))
        self.assertEqual(json.loads(params[9]),['fish'])
        self.assertTrue(params[17])
    def test_15_unrecognized_rename_requires_review(self):
        row=(1,'Renamed',True,False,'Origin','red','cabernet','fruit','full','dry','Pairs with steak',None)
        pg=FakePG(legacy_rows=(row,),aliases=(('original','stable-id'),),old=(('stable-id','Original','Pronun',[],[],False),))
        with self.assertRaisesRegex(CatalogDataError,'manual ID review'):
            PostgresCatalogStore(lambda:pg).import_legacy()
    def test_05_manual_approval_writes_tag_gate(self):
        pg=FakePG(suggested=('red_meat','fish'))
        PostgresCatalogStore(lambda:pg).approve_pairings('venue','id',['red_meat'])
        args=[p for q,p in pg.queries if q.startswith('update portfolio_v2.wines set pairings=')]
        self.assertEqual(json.loads(args[0][0]),['red_meat'])
        self.assertEqual(args[0][1:],('venue','id'))
        pg2=FakePG(suggested=('fish',))
        with self.assertRaisesRegex(ValueError,'Tags require review'):
            PostgresCatalogStore(lambda:pg2).approve_pairings('venue','id',['red_meat'])
    def test_15_review_alias_can_map_new_name_to_existing_identity(self):
        pg=FakePG()
        PostgresCatalogStore(lambda:pg).bind_legacy_name('venue','Corrected label','stable-id')
        self.assertTrue(any(q.startswith('insert into portfolio_v2.legacy_identity_map') and p[2]=='stable-id' for q,p in pg.queries))
    def test_003_migration_contains_review_and_alias_gate(self):
        script=(Path(__file__).resolve().parents[1]/'migrations/003_legacy_review.sql').read_text()
        self.assertIn('legacy_identity_map',script)
        self.assertIn('suggested_pairings',script)
        self.assertIn('pairings_reviewed',script)
        self.assertIn("pairings='[]'::jsonb",script)

if __name__=='__main__':unittest.main()
