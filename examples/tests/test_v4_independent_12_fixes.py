"""Independent regressions for the v3 twelve-finding audit (fully offline)."""
from __future__ import annotations
from claim_fixture import claim_nonce
import csv
import io
import json
import os
import pathlib
import sqlite3
import stat
import sys
import tempfile
import unittest
import urllib.error
import zipfile
from pathlib import Path

EXAMPLES = Path(__file__).resolve().parents[1]
ROOT = EXAMPLES.parent
sys.path.insert(0,str(EXAMPLES))
sys.path.insert(0,str(ROOT/'scripts'))
from pipeline import Preferences,load_catalog,select_candidates,CatalogDataError,ContractError
from delivery import (EventStore,process_event,fingerprint_for,KnownNotAccepted,
                      normalize_recipient)
from catalog_store import CatalogStore
from providers import StructuredRecommender,PostmarkSender
from runtime import config_bindings
from postgres_catalog import REQUIRED_SCHEMA_COLUMNS,NULLABLE_WINE_COLUMNS,_validate_schema_metadata
from release_gate import scan,scan_distribution

class Reply:
    status=200
    def __init__(self,payload):self.raw=json.dumps(payload).encode()
    def __enter__(self):return self
    def __exit__(self,*_):return False
    def read(self,n):return self.raw[:n]

class V4Regressions(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.base=Path(self.tmp.name)
        self.pref=Preferences(category='red',sweetness='dry')

    def test_01_release_scan_rejects_forbidden_tree_members(self):
        (self.base/'.git').mkdir()
        (self.base/'.git'/'config').write_text('sensitive artifact')
        (self.base/'__pycache__').mkdir()
        (self.base/'__pycache__'/'secrets.py').write_text('private compiled input')
        findings=scan(self.base)
        self.assertTrue(any('.git' in x for x in findings),findings)
        self.assertTrue(any('__pycache__' in x for x in findings),findings)

    def test_01_release_scan_rejects_forbidden_archive_members(self):
        p=self.base/'bad.zip'
        with zipfile.ZipFile(p,'w') as z:
            z.writestr('restaurant-wine-recommendation-reference/.git/config','sensitive artifact')
            z.writestr('restaurant-wine-recommendation-reference/__pycache__/secrets.py','private input')
        findings=scan_distribution(p)
        self.assertTrue(any('.git/' in x for x in findings),findings)
        self.assertTrue(any('__pycache__/' in x for x in findings),findings)

    def _env(self,secret2='this-is-an-independent-second-secret',duplicate_name=False):
        entries={
          'first':{'form_id':'A','venue_id':'venue_a','secret_env':'TYPEFORM_SECRET_ONE'},
          'second':{'form_id':'B','venue_id':'venue_b',
                    'secret_env':'TYPEFORM_SECRET_ONE' if duplicate_name else 'TYPEFORM_SECRET_TWO'}}
        return {'TYPEFORM_BINDINGS_JSON':json.dumps(entries),
                'TYPEFORM_SECRET_ONE':'this-is-the-first-signing-secret',
                'TYPEFORM_SECRET_TWO':secret2}

    def test_02_distinct_signing_key_and_env_per_form(self):
        with self.assertRaises(RuntimeError):config_bindings(self._env(duplicate_name=True))
        with self.assertRaises(RuntimeError):config_bindings(self._env(secret2='this-is-the-first-signing-secret'))
        self.assertEqual(len(config_bindings(self._env())),2)

    def test_03_direct_entrypoint_persists_retryable_payload(self):
        class BrokenCatalog:
            def has_venue(self,v):return True
            def wines_for_venue(self,v):raise CatalogDataError('temporary storage outage')
        store=EventStore(self.base/'event.db');self.addCleanup(store.close)
        result=process_event(store,token='direct',claimed_venue='venue_a',authenticated_venue='venue_a',
            recipient_key='Address@example.test',preferences=self.pref,now=1,
            catalog_store=BrokenCatalog(),sender=lambda _:self.fail('Unexpected email'))
        self.assertEqual(result,'retryable_failure')
        self.assertEqual(store.due_requests(now=32)[0][0],'direct')

    def test_03_raw_claim_without_payload_is_quarantined(self):
        store=EventStore(self.base/'event.db');self.addCleanup(store.close)
        with self.assertRaisesRegex(ValueError,'verified durable admission'):
            store.claim('untrusted','hash',now=1)
        self.assertEqual(store.due_requests(now=33),())
        self.assertIsNone(store.summary('untrusted'))

    def test_04_malformed_openai_message_is_a_contract_failure(self):
        selection=select_candidates(load_catalog(),'venue_a',self.pref)
        model=StructuredRecommender('synthetic',opener=lambda req,timeout:Reply({'choices':[{'message':[]}]}))
        with self.assertRaises(ContractError):model(selection,self.pref)

    def test_04_malformed_openai_uses_grounded_delivery_fallback(self):
        catalog=CatalogStore(self.base/'catalog.db');self.addCleanup(catalog.close)
        catalog.replace_venue('venue_a',[w for w in load_catalog() if w.restaurant_id=='venue_a'])
        model=StructuredRecommender('synthetic',opener=lambda req,timeout:Reply({'choices':[{'message':[]}]}))
        sent=[];store=EventStore(self.base/'events.db');self.addCleanup(store.close)
        result=process_event(store,token='bad_model',claimed_venue='venue_a',authenticated_venue='venue_a',
            recipient_key='r@example.test',preferences=self.pref,now=1,catalog_store=catalog,
            recommender=model,sender=lambda recs:sent.append(recs) or 'accept-ref')
        self.assertEqual(result,'provider_accepted')
        self.assertEqual(len(sent[0]),3)

    def test_05_explicit_rate_limit_allows_bounded_retry_after(self):
        def opened(req,timeout):
            raise urllib.error.HTTPError(req.full_url,429,'rate limit',{'Retry-After':'120'},io.BytesIO(b'{}'))
        sender=PostmarkSender('token-placeholder','Sender@example.test',opener=opened)
        with self.assertRaises(KnownNotAccepted) as caught:sender.send('Recipient@example.test',[])
        self.assertEqual(caught.exception.retry_after,120.0)
        store=EventStore(self.base/'events.db');self.addCleanup(store.close)
        store.enqueue_request(token='rate',venue='venue_a',recipient='r@example.test',preferences=self.pref,now=1)
        claim=store.claim('rate',fingerprint_for('venue_a','r@example.test',self.pref),now=1)
        store.mark_dispatch_started('rate',claim.attempt,now=2,nonce=claim_nonce(store,'rate',claim.attempt))
        self.assertEqual(store.mark_known_not_accepted('rate',claim.attempt,now=2,retry_after=120,nonce=claim_nonce(store,'rate',claim.attempt)),'retryable_failure')
        self.assertEqual(store.due_requests(now=121),())
        self.assertEqual(store.due_requests(now=122)[0][0],'rate')

    def test_06_sqlite_rejects_tag_scalars_at_schema_boundary(self):
        catalog=CatalogStore(self.base/'catalog.db');self.addCleanup(catalog.close)
        wine=load_catalog()[0]
        catalog.replace_venue(wine.restaurant_id,[wine])
        with self.assertRaises(sqlite3.IntegrityError):
            with catalog._connect() as conn:conn.execute('UPDATE catalog SET characteristics=?',(json.dumps('bold'),))
        self.assertEqual(catalog.wines_for_venue(wine.restaurant_id),(wine,))
        with self.assertRaises(CatalogDataError):catalog._tags(json.dumps('bold'))

    def test_07_duplicate_catalog_headers_rejected(self):
        with (EXAMPLES/'synthetic_wines.csv').open(newline='',encoding='utf-8') as f:rows=list(csv.reader(f))
        col=rows[0].index('wine_id')
        rows[0].insert(col+1,'wine_id')
        for row in rows[1:]:row.insert(col+1,'forged_identity')
        with (self.base/'bad.csv').open('w',newline='',encoding='utf-8') as f:csv.writer(f).writerows(rows)
        with self.assertRaises(CatalogDataError):load_catalog(self.base/'bad.csv')

    def test_08_operator_cannot_accept_unsent_permanent_failure(self):
        store=EventStore(self.base/'events.db');self.addCleanup(store.close)
        store.enqueue_request(token='x',venue='venue_a',recipient='r@example.test',preferences=self.pref,now=1)
        claim=store.claim('x',fingerprint_for('venue_a','r@example.test',self.pref),now=1)
        store.mark_permanent_failure('x',claim.attempt,now=2,nonce=claim_nonce(store,'x',claim.attempt))
        with self.assertRaisesRegex(ValueError,'no submission'):
            store.reconcile('x',now=3,decision='confirmed_accepted',provider_ref='invented-ref',evidence='Checked provider message dashboard')
        self.assertEqual(store.summary('x')[0],'permanent_failure')

    def test_09_preserves_local_part_and_only_lowercases_domain(self):
        self.assertEqual(normalize_recipient(' Case.Name@Example.TEST ',require_email=True),'Case.Name@example.test')
        self.assertNotEqual(fingerprint_for('venue_a','Case.Name@Example.test',self.pref),
                            fingerprint_for('venue_a','case.name@example.test',self.pref))
        self.assertEqual(fingerprint_for('venue_a','Case.Name@EXAMPLE.TEST',self.pref),
                         fingerprint_for('venue_a','Case.Name@example.test',self.pref))

    def test_10_postgres_type_constraint_and_index_contract(self):
        columns=[(table,col,kind,
            'YES' if table in ('wines','wine_review_archive') and col in NULLABLE_WINE_COLUMNS else 'NO')
            for table,spec in REQUIRED_SCHEMA_COLUMNS.items() for col,kind in spec.items()]
        from postgres_catalog import _normalized_check_expression
        primary={
          'venues':('venue_id',),'wines':('venue_id','wine_id'),
          'legacy_identity_map':('venue_id','legacy_name'),
          'wine_review_archive':('venue_id','wine_id'),
          'schema_migrations':('version',),
      'archive_text_quarantine_v16':('venue_id','wine_id'),
        }
        constraints=[(table,table+'_pkey','p','PRIMARY KEY ('+', '.join(cols)+')',True,cols,None,())
                     for table,cols in primary.items()]
        constraints += [(table,table+'_venue_id_fkey','f',
                         'FOREIGN KEY (venue_id) REFERENCES venues(venue_id)',True,
                         ('venue_id',),'venues',('venue_id',))
             for table in ('wines','legacy_identity_map','wine_review_archive')]
        check_definitions=[
            ('wine_review_archive','wine_review_archive_tags_valid',
             'CHECK (valid_tag_array(characteristics) AND valid_tag_array(pairings) AND valid_tag_array(suggested_pairings))'),
            ('wines','category_chk',"CHECK (category IN ('red','white','sparkling','rose'))"),
            ('wines','sweetness_chk',"CHECK (sweetness IN ('dry','semi-dry','semi-sweet','sweet'))"),
            ('wines','availability_chk','CHECK (available_by_glass OR available_by_bottle)'),
            ('wines','tags_a_chk','CHECK (portfolio_v2.valid_tag_array(characteristics))'),
            ('wines','tags_b_chk','CHECK (portfolio_v2.valid_tag_array(pairings))'),
            ('wines','tags_c_chk','CHECK (portfolio_v2.valid_tag_array(suggested_pairings))'),
            ('wines','wines_visible_text_contract_v16','CHECK ('+
             ' AND '.join(f'portfolio_v2.valid_visible_wine_text({field})' for field in ('wine_id','wine_name','pronunciation','origin','wine_type'))+')'),
            ('wine_review_archive','wine_review_archive_visible_text_contract_v16',
             'CHECK (portfolio_v2.valid_visible_wine_text(pronunciation))'),
            ('wines','wine_required_text_contract_v15','CHECK ('+
         ' AND '.join(f'portfolio_v2.valid_required_wine_text({field})'
                      for field in ('wine_id','wine_name','pronunciation','origin','wine_type'))+')'),
        ('wines','wine_required_text_valid','CHECK ('+
             ' AND '.join(part for field in ('wine_id','wine_name','pronunciation','origin','wine_type')
                         for part in (f'length(btrim({field})) BETWEEN 1 AND 1500',
                                      f'length({field}) <= 1500'))+')'),
        ]
        constraints += [(table,name,'c',definition,True,(),None,())
                        for table,name,definition in check_definitions]
        indexes=[('wines','wines_venue_category','CREATE INDEX wines_venue_category ON portfolio_v2.wines (venue_id, category, sweetness)')]
        _validate_schema_metadata(columns,constraints,indexes)
        mutated=[x if x[:2]!=('wines','sweetness') else ('wines','sweetness','text','YES') for x in columns]
        with self.assertRaisesRegex(RuntimeError,'sweetness'):_validate_schema_metadata(mutated,constraints,indexes)
        with self.assertRaisesRegex(RuntimeError,'index'):_validate_schema_metadata(columns,constraints,[])
        with self.assertRaisesRegex(RuntimeError,'check'):_validate_schema_metadata(columns,[c for c in constraints if c[1]!='tags_c_chk'],indexes)

    @unittest.skipUnless(os.name=='posix','POSIX permissions test')
    def test_11_all_new_directory_levels_private(self):
        old=os.umask(0o022)
        try:
            target=self.base/'layer_one'/'layer_two'/'events.sqlite3'
            store=EventStore(target);store.close()
            self.assertEqual(stat.S_IMODE(target.parent.parent.stat().st_mode),0o700)
            self.assertEqual(stat.S_IMODE(target.parent.stat().st_mode),0o700)
            self.assertEqual(stat.S_IMODE(target.stat().st_mode),0o600)
        finally:os.umask(old)

    def test_12_current_model_contract_documented(self):
        doc=(ROOT/'docs'/'candidate-retrieval-and-ranking.md').read_text()
        self.assertNotIn('plus explanations in a machine-validated',doc)
        self.assertIn('descriptions locally',doc)

if __name__=='__main__':unittest.main()
