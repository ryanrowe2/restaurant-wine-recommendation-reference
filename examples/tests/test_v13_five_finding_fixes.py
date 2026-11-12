"""Five independent v12 audit findings; all fixtures are synthetic and offline."""
import json
from contextlib import closing
import sqlite3
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'examples'))
sys.path.insert(0,str(Path(__file__).parent))
from catalog_store import CatalogStore
from pipeline import CatalogDataError,load_catalog,MAX_CATALOG_ROWS
from postgres_catalog import (PostgresCatalogStore,REQUIRED_SCHEMA_COLUMNS,
    NULLABLE_WINE_COLUMNS,_validate_schema_metadata,_assert_database_checks_enforced)
from test_v12_six_finding_fixes import StatefulReviewPG
from test_v3_postgres_contract import FakePG


def valid_schema():
    columns=[(table,col,typ,'YES' if table in ('wines','wine_review_archive')
             and col in NULLABLE_WINE_COLUMNS else 'NO')
             for table,fields in REQUIRED_SCHEMA_COLUMNS.items() for col,typ in fields.items()]
    primary={
      'venues':('venue_id',),'wines':('venue_id','wine_id'),
      'legacy_identity_map':('venue_id','legacy_name'),
      'wine_review_archive':('venue_id','wine_id'),
      'schema_migrations':('version',),
      'archive_text_quarantine_v16':('venue_id','wine_id'),
    }
    constraints=[(table,table+'_pkey','p',f'PRIMARY KEY ({", ".join(keys)})',True,keys,None,())
                 for table,keys in primary.items()]
    constraints += [(table,table+'_venue_id_fkey','f',
                    'FOREIGN KEY (venue_id) REFERENCES venues(venue_id)',True,
                    ('venue_id',),'venues',('venue_id',))
                    for table in ('wines','legacy_identity_map','wine_review_archive')]
    defs=[
        ('wines','category_chk',"CHECK (category IN ('red','white','sparkling','rose'))"),
        ('wines','sweetness_chk',"CHECK (sweetness IN ('dry','semi-dry','semi-sweet','sweet'))"),
        ('wines','availability_chk','CHECK (available_by_glass OR available_by_bottle)'),
        ('wines','tags_a_chk','CHECK (portfolio_v2.valid_tag_array(characteristics))'),
        ('wines','tags_b_chk','CHECK (portfolio_v2.valid_tag_array(pairings))'),
        ('wines','tags_c_chk','CHECK (portfolio_v2.valid_tag_array(suggested_pairings))'),
        ('wine_review_archive','wine_review_archive_tags_valid',
         'CHECK (valid_tag_array(characteristics) AND valid_tag_array(pairings) AND valid_tag_array(suggested_pairings))'),
        ('wines','wine_required_text_contract_v15','CHECK ('+
         ' AND '.join(f'portfolio_v2.valid_required_wine_text({field})'
                      for field in ('wine_id','wine_name','pronunciation','origin','wine_type'))+')'),
        ('wines','wines_visible_text_contract_v16','CHECK ('+
         ' AND '.join(f'portfolio_v2.valid_visible_wine_text({field})' for field in ('wine_id','wine_name','pronunciation','origin','wine_type'))+')'),
        ('wine_review_archive','wine_review_archive_visible_text_contract_v16',
         'CHECK (portfolio_v2.valid_visible_wine_text(pronunciation))'),
        ('wines','wine_required_text_valid','CHECK ('+
         ' AND '.join(part for field in ('wine_id','wine_name','pronunciation','origin','wine_type')
                    for part in (f'length(btrim({field})) BETWEEN 1 AND 1500',f'length({field}) <= 1500'))+')'),
    ]
    constraints += [(table,name,'c',definition,True,(),None,()) for table,name,definition in defs]
    indexes=[('wines','wines_venue_category','CREATE INDEX wines_venue_category ON portfolio_v2.wines (venue_id, category, sweetness)')]
    return columns,constraints,indexes


class SimulatedCheckFailure(Exception):
    pgcode='23514'

class ConstraintCursor:
    def __init__(self,reject_all=True):
        self.calls=[];self.reject_all=reject_all
    def execute(self,sql,params=None):
        self.calls.append((sql,params))
        if sql.lstrip().lower().startswith('insert into portfolio_v2.wines') and self.reject_all:
            raise SimulatedCheckFailure('synthetic PostgreSQL CHECK violation')


class V13FiveFindings(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.wine=next(w for w in load_catalog() if w.restaurant_id=='venue_a')

    def test_p12_01_reviewed_characteristics_survive_zero_stock_reactivation(self):
        pg=StatefulReviewPG(self.wine)
        pg.live[self.wine.wine_id]['characteristics']=['bold']
        store=PostgresCatalogStore(lambda:pg)
        store.replace_venue('venue_a',[],allow_empty=True)
        self.assertEqual(pg.archived[self.wine.wine_id]['characteristics'],['bold'])
        store.replace_venue('venue_a',[replace(self.wine,characteristics=frozenset())])
        self.assertEqual(pg.live[self.wine.wine_id]['characteristics'],['bold'])

    def test_p12_01_characteristics_preserved_on_ordinary_replacement(self):
        pg=StatefulReviewPG(self.wine)
        pg.live[self.wine.wine_id]['characteristics']=['bold']
        PostgresCatalogStore(lambda:pg).replace_venue('venue_a',[
           replace(self.wine,characteristics=frozenset({'earthy'}))])
        self.assertEqual(set(pg.live[self.wine.wine_id]['characteristics']),{'bold','earthy'})

    def test_p12_02_key_columns_and_fk_source_must_be_exact(self):
        columns,constraints,indexes=valid_schema()
        _validate_schema_metadata(columns,constraints,indexes)
        corrupt=[(*row[:5],('venue_id','wine_id','wine_name'),*row[6:])
                 if row[:2]==('wines','wines_pkey') else row for row in constraints]
        with self.assertRaisesRegex(RuntimeError,'primary key'):
            _validate_schema_metadata(columns,corrupt,indexes)
        corrupt=[(*row[:5],('wine_id',),*row[6:])
                 if row[:2]==('wines','wines_venue_id_fkey') else row for row in constraints]
        with self.assertRaisesRegex(RuntimeError,'foreign key'):
            _validate_schema_metadata(columns,corrupt,indexes)
        corrupt=[(*row[:6],'other_table',row[7])
                 if row[:2]==('wines','wines_venue_id_fkey') else row for row in constraints]
        with self.assertRaisesRegex(RuntimeError,'foreign key'):
            _validate_schema_metadata(columns,corrupt,indexes)

    def test_p12_02_true_or_check_and_extra_category_rejected(self):
        columns,constraints,indexes=valid_schema()
        for broken in ("CHECK (TRUE OR category IN ('red','white','sparkling','rose'))",
                       "CHECK (category IN ('red','white','sparkling','rose','blue'))"):
            modified=[(*row[:3],broken,*row[4:]) if row[:2]==('wines','category_chk') else row
                      for row in constraints]
            with self.subTest(broken=broken),self.assertRaisesRegex(RuntimeError,'check'):
                _validate_schema_metadata(columns,modified,indexes)

    def test_p12_02_behavioral_dry_run_uses_rollback_savepoints(self):
        cursor=ConstraintCursor()
        _assert_database_checks_enforced(cursor)
        commands=[sql for sql,_ in cursor.calls]
        self.assertEqual(len([sql for sql in commands if 'INSERT INTO portfolio_v2.wines' in sql]),17)
        self.assertEqual(sum(sql=='ROLLBACK TO SAVEPOINT palate_schema_case' for sql in commands),17)
        self.assertEqual(commands[-2:],["ROLLBACK TO SAVEPOINT palate_schema_audit",
                                         "RELEASE SAVEPOINT palate_schema_audit"])
        with self.assertRaisesRegex(RuntimeError,'ineffective persisted CHECK'):
            _assert_database_checks_enforced(ConstraintCursor(reject_all=False))

    def test_p12_03_replacement_existing_inventory_is_bounded(self):
        pg=FakePG(provenance=tuple((f'x-{i}',None,None,None,None,[],False,[])
                            for i in range(MAX_CATALOG_ROWS+1)))
        with self.assertRaisesRegex(CatalogDataError,'administrative row limit'):
            PostgresCatalogStore(lambda:pg).replace_venue('venue_a',[self.wine])
        select=[(sql,args) for sql,args in pg.queries if sql.startswith('select wine_id,source_metadata')]
        self.assertEqual(len(select),1)
        self.assertIn('limit %s',select[0][0])
        self.assertEqual(select[0][1],('venue_a',MAX_CATALOG_ROWS+1))
        self.assertFalse(any(sql.startswith('delete from portfolio_v2.wines') for sql,_ in pg.queries))

    def test_p12_03_legacy_alias_history_bounded_before_writes(self):
        legacy=(1,'Fictional Cabernet',True,False,'Imaginary','red','cabernet','body','full','dry','Steak',None)
        pg=FakePG(legacy_rows=(legacy,),aliases=tuple((f'alias-{i}',f'old-{i}')
                                                    for i in range(MAX_CATALOG_ROWS+1)))
        with self.assertRaisesRegex(CatalogDataError,'identity history'):
            PostgresCatalogStore(lambda:pg).import_legacy()
        queries=[(sql,params) for sql,params in pg.queries if sql.startswith('select legacy_name,wine_id')]
        self.assertEqual(len(queries),1)
        self.assertIn('limit %s',queries[0][0])
        self.assertFalse(any(sql.startswith('delete from portfolio_v2.wines') for sql,_ in pg.queries))

    def test_p12_03_legacy_prior_active_catalog_bounded(self):
        legacy=(1,'Fictional Cabernet',True,False,'Imaginary','red','cabernet','body','full','dry','Steak',None)
        pg=FakePG(legacy_rows=(legacy,),aliases=(('fictional cabernet','abc'),),
             old=tuple((f'old-{i}','Name','Pronunciation',[],[],False)
                       for i in range(MAX_CATALOG_ROWS+1)))
        with self.assertRaisesRegex(CatalogDataError,'administrative row limit'):
            PostgresCatalogStore(lambda:pg).import_legacy()
        self.assertFalse(any(sql.startswith('delete from portfolio_v2.wines') for sql,_ in pg.queries))

    def _catalog(self):
        store=CatalogStore(Path(self.tmp.name)/'db.sqlite3')
        self.addCleanup(store.close)
        store.replace_venue('venue_a',[self.wine])
        return store

    def test_p12_04_null_utf8_and_embedded_nul_fail_closed_on_direct_writes(self):
        for sql_value in ("CAST(X'80' AS TEXT)","'X'||char(0)||'Y'", "'X'||char(0)||'Y'||zeroblob(2000)"):
            with self.subTest(sql_value=sql_value):
                store=self._catalog()
                with self.assertRaises(sqlite3.DatabaseError),store._connect() as conn:
                    conn.execute(f'UPDATE catalog SET wine_name={sql_value} WHERE restaurant_id=?',('venue_a',))
                self.assertEqual(store.wines_for_venue('venue_a')[0].wine_name,self.wine.wine_name)

    def test_p12_04_direct_insert_and_raw_unregistered_writer_fail_closed(self):
        store=self._catalog()
        with store._connect() as conn:
            args=(self.wine.restaurant_id,'second',self.wine.wine_name,self.wine.pronunciation,
                  self.wine.origin,self.wine.category,self.wine.type,self.wine.sweetness,
                  '[]','[]',1,0)
            with self.assertRaises(sqlite3.DatabaseError):
                conn.execute('''INSERT INTO catalog(restaurant_id,wine_id,wine_name,pronunciation,origin,
                  category,type,sweetness,characteristics,pairings,available_by_glass,available_by_bottle)
                  VALUES(?, ?,CAST(X'80' AS TEXT), ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  args[:2]+args[3:])
        with closing(sqlite3.connect(store._db.path)) as raw:
            with self.assertRaises(sqlite3.DatabaseError):
                raw.execute("UPDATE catalog SET wine_name='Valid Name' WHERE restaurant_id='venue_a'")

    def test_p12_04_corrupt_existing_text_raises_controlled_catalog_error(self):
        store=self._catalog()
        with store._connect() as conn:
            conn.execute('DROP TRIGGER catalog_required_update')
            conn.execute("UPDATE catalog SET wine_name=CAST(X'80' AS TEXT) WHERE restaurant_id='venue_a'")
        with self.assertRaisesRegex(CatalogDataError,'cannot be decoded'):
            store.wines_for_venue('venue_a')
        with self.assertRaisesRegex(CatalogDataError,'cannot be decoded'):
            CatalogStore(store._db.path)

    def test_p12_05_readme_current_version_and_history(self):
        docs=(ROOT/'README.md').read_text('utf-8')
        self.assertIn('[historical v13 remediation](docs/v13-five-finding-remediation.md)',docs)
        self.assertIn('[historical v14 remediation](docs/v14-four-finding-remediation.md)',docs)
        self.assertIn('[Historical v16 remediation](docs/v16-eight-finding-remediation.md)',docs)
        self.assertIn('[v17 consolidated remediation record](docs/v17-consolidated-remediation.md)',docs)
        self.assertIn('[historical v15 remediation](docs/v15-five-finding-remediation.md)',docs)
        self.assertIn('[historical v11 remediation](docs/v11-six-finding-remediation.md)',docs)
        self.assertNotIn('[Current v11 remediation]',docs)

if __name__=='__main__':unittest.main()
