"""v16 regression controls for eight independent v15 findings; synthetic only."""
from __future__ import annotations
import base64
import json
import re
import sqlite3
import sys
import tempfile
import unittest
import unicodedata
from pathlib import Path
from dataclasses import replace

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'examples'),str(ROOT/'scripts'),str(Path(__file__).parent)]
from delivery import EventStore, fingerprint_for
from pipeline import (Wine,Preferences,load_catalog,load_catalog as csv_load,InputError,
                      CatalogDataError, valid_required_wine_text, valid_venue_id, select_candidates)
from catalog_store import CatalogStore
from postgres_catalog import PostgresCatalogStore,_validate_schema_metadata,_assert_database_positive_controls
from release_gate import scan
from test_v13_five_finding_fixes import valid_schema
from test_v14_four_finding_fixes import PositiveProbeCursor

class V16EightFindings(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory(prefix='palate-v16-')
        self.addCleanup(temp.cleanup)
        self.tmp=Path(temp.name)
        self.wines=[w for w in load_catalog() if w.restaurant_id=='venue_a']

    def _event(self,token='v16fence'):
        store=EventStore(self.tmp/(token+'.sqlite'))
        self.addCleanup(store.close)
        prefs=Preferences(category='red',sweetness='dry')
        store.enqueue_request(token=token,venue='venue_a',recipient='synthetic@example.test',preferences=prefs,now=1)
        return store, store.claim(token,fingerprint_for('venue_a','synthetic@example.test',prefs),now=2)

    def test_p15_01_svg_embedded_png_and_secret_are_rejected(self):
        root=self.tmp/'repository';(root/'diagrams').mkdir(parents=True)
        fake_png=b'\x89PNG\r\n\x1a\nComment\0'+('sk-proj-'+'a'*40).encode()
        svg='<svg xmlns="http://www.w3.org/2000/svg"><image href="data:image/png;base64,'+base64.b64encode(fake_png).decode()+'" /></svg>'
        (root/'diagrams'/'probe.svg').write_text(svg)
        self.assertNotIn('sk-proj-',svg)
        self.assertTrue(any('unsafe SVG' in issue for issue in scan(root)))

    def test_p15_02_svg_active_content_event_handlers_and_external_links_are_rejected(self):
        root=self.tmp/'repo';(root/'diagrams').mkdir(parents=True)
        bad=(
            '<script>alert(1)</script>',
            '<foreignObject><div>unsafe</div></foreignObject>',
            '<rect width="1" onload="alert(1)" />',
            '<image href="https://example.invalid/p.png" />',
            '<rect width="1" style="fill:url(https://example.invalid)" />',
            '<use href="#target" />',
        )
        for payload in bad:
            (root/'diagrams'/'probe.svg').write_text('<svg xmlns="http://www.w3.org/2000/svg">'+payload+'</svg>')
            with self.subTest(payload=payload):
                self.assertTrue(any('unsafe SVG' in issue for issue in scan(root)))
        (root/'diagrams'/'probe.svg').write_text('<!DOCTYPE svg [<!ENTITY fake SYSTEM "file:///etc/passwd">]><svg xmlns="http://www.w3.org/2000/svg" />')
        self.assertTrue(scan(root))

    def test_p15_02_authored_svg_diagrams_still_pass(self):
        # Existing documentation diagrams remain supported under the strict subset.
        import shutil
        root=self.tmp/'authored';(root/'diagrams').mkdir(parents=True)
        for file in (ROOT/'diagrams').glob('*.svg'):
            shutil.copy2(file,root/'diagrams'/file.name)
        self.assertFalse(scan(root))

    def test_p15_03_provider_acceptance_requires_exact_claim_nonce(self):
        store,claim=self._event()
        with self.assertRaises(RuntimeError):
            store.mark_dispatch_started('v16fence',claim.attempt,now=3)
        with self.assertRaises(RuntimeError):
            store.mark_dispatch_started('v16fence',claim.attempt,now=3,nonce='a'*32)
        self.assertEqual(store.summary('v16fence')[0],'sending')
        store.mark_dispatch_started('v16fence',claim.attempt,now=3,nonce=claim.nonce)
        with self.assertRaises(RuntimeError):
            store.mark_provider_accepted('v16fence',claim.attempt,'forged',now=4)
        with self.assertRaises(RuntimeError):
            store.mark_provider_accepted('v16fence',claim.attempt,'forged',now=4,nonce='0'*32)
        self.assertEqual(store.summary('v16fence')[0],'submitting')
        store.mark_provider_accepted('v16fence',claim.attempt,'real-receipt',now=4,nonce=claim.nonce)
        self.assertEqual(store.summary('v16fence')[2],'real-receipt')

    def test_p15_03_every_worker_transition_and_model_audit_requires_nonce(self):
        store,claim=self._event('v16all')
        calls=[
            lambda:store.heartbeat('v16all',claim.attempt,now=3),
            lambda:store.mark_permanent_failure('v16all',claim.attempt,now=3),
            lambda:store.mark_retryable_failure('v16all',claim.attempt,now=3),
            lambda:store.record_model_degradation('v16all',claim.attempt,now=3,category='contract'),
            lambda:store.mark_uncertain('v16all',claim.attempt,now=3),
            lambda:store.mark_known_not_accepted('v16all',claim.attempt,now=3),
        ]
        for call in calls:
            with self.subTest(transition=call),self.assertRaises(RuntimeError):call()
        self.assertEqual(store.summary('v16all')[0],'sending')
        store.record_model_degradation('v16all',claim.attempt,now=3,category='contract',nonce=claim.nonce)
        store.mark_permanent_failure('v16all',claim.attempt,now=4,nonce=claim.nonce)
        self.assertEqual(store.summary('v16all')[0],'permanent_failure')

    def test_p15_04_invisible_and_bidi_catalog_fields_rejected(self):
        for value in ('\u200b','\ufeff','\u202eXYZ','X\u200cY','\u034f','\ufe0f','\U000e0100','\u2060','a\u200db'):
            with self.subTest(text=repr(value)):
                self.assertFalse(valid_required_wine_text(value))
                with self.assertRaises(CatalogDataError):replace(self.wines[0],wine_name=value)
        self.assertTrue(valid_required_wine_text('Café Sonoma 2025'))

    def test_p15_04_sqlite_direct_writes_fail_closed(self):
        store=CatalogStore(self.tmp/'unicode.sqlite');self.addCleanup(store.close)
        store.replace_venue('venue_a',[self.wines[0]])
        for value in ('\u200b','\u202eSecret','\u034f','\U000e0100'):
            with store._connect() as conn:
                with self.subTest(value=repr(value)),self.assertRaises(sqlite3.DatabaseError):
                    conn.execute('UPDATE catalog SET wine_name=? WHERE restaurant_id=?',(value,'venue_a'))
        self.assertEqual(store.wines_for_venue('venue_a')[0].wine_name,self.wines[0].wine_name)

    def test_p15_05_bad_archived_pronunciation_uses_valid_incoming(self):
        wine=self.wines[0]
        class Cursor:
            def __init__(self):self.selects=0;self.insert=[]
            def execute(self,sql,params=None):
                if 'INSERT INTO portfolio_v2.wines(' in sql:self.insert.append(params)
            def fetchone(self):return ('venue_a',)
            def fetchmany(self,n):
                self.selects+=1
                if self.selects==1:return []
                if self.selects==2:return [(wine.wine_id,'\t','[]','[]',False,None,None,None,None,'[]')]
                return []
            def __enter__(self):return self
            def __exit__(self,*args):return False
        class Connection:
            def __init__(self):self.cur=Cursor()
            def cursor(self):return self.cur
            def __enter__(self):return self
            def __exit__(self,*args):return False
            def close(self):pass
        conn=Connection()
        self.assertEqual(PostgresCatalogStore(lambda:conn).replace_venue('venue_a',[wine]),1)
        self.assertTrue(conn.cur.insert)
        self.assertEqual(conn.cur.insert[0][3],wine.pronunciation)

    def test_p15_05_additive_quarantine_migration_preserves_bad_text(self):
        sql=(ROOT/'examples/migrations/008_visible_display_text.sql').read_text()
        self.assertIn('CREATE TABLE portfolio_v2.archive_text_quarantine_v16',sql)
        self.assertIn('original_pronunciation',sql)
        self.assertIn('NOT portfolio_v2.valid_visible_wine_text(pronunciation)',sql)
        self.assertIn('wine_review_archive_visible_text_contract_v16',sql)
        self.assertIn('wines_visible_text_contract_v16',sql)
        self.assertIn("'Pronunciation requires review'",sql)
        for i in range(1,8):
            self.assertTrue(any((ROOT/'examples/migrations').glob(f'{i:03d}_*.sql')))

    def test_p15_06_duplicate_and_noncanonical_tags_are_rejected_at_sql_boundary(self):
        store=CatalogStore(self.tmp/'tags.sqlite');self.addCleanup(store.close)
        store.replace_venue('venue_a',[self.wines[0]])
        bad=(' ["bold"]','["bold","bold"]','[ "bold" ]','["z", "a"]','["red_meat", "bold"]','["bold"  ]')
        for tags in bad:
            with self.subTest(tags=tags),store._connect() as db,self.assertRaises(sqlite3.DatabaseError):
                db.execute('UPDATE catalog SET characteristics=? WHERE restaurant_id=?',(tags,'venue_a'))
        valid=json.dumps(['bold','red_meat'])
        self.assertEqual(store._valid_tag_array(valid),1)
        self.assertEqual(store.wines_for_venue('venue_a')[0].characteristics,self.wines[0].characteristics)

    def test_p15_06_old_duplicate_tag_database_rejected_at_upgrade(self):
        path=self.tmp/'legacy_tags.sqlite'
        store=CatalogStore(path);store.replace_venue('venue_a',[self.wines[0]])
        with store._connect() as db:
            db.execute('DROP TRIGGER catalog_validate_tags_update')
            db.execute('UPDATE catalog SET characteristics=? WHERE restaurant_id=?',('["bold", "bold"]','venue_a'))
        store.close()
        with self.assertRaisesRegex(CatalogDataError,'invalid tags'):
            CatalogStore(path)

    def test_p15_07_venue_contract_identical_across_csv_wine_and_selection(self):
        venue='v'*129
        self.assertFalse(valid_venue_id(venue))
        with self.assertRaisesRegex(CatalogDataError,'venue identity'):
            replace(self.wines[0],restaurant_id=venue)
        path=self.tmp/'invalid_venue.csv'
        path.write_text((ROOT/'examples/synthetic_wines.csv').read_text().replace('venue_a',venue))
        with self.assertRaises(CatalogDataError):csv_load(path)
        self.assertTrue(valid_venue_id('venue_a'))
        from webhook import FormBinding
        with self.assertRaisesRegex(ValueError,'binding'):
            FormBinding('form_1','\u200b',b'synthetic-binding-secret')
        with self.assertRaisesRegex(ValueError,'hidden restaurant'):
            FormBinding('form_1','venue_a',b'synthetic-binding-secret',hidden_restaurant_id='\u202ev')
        self.assertEqual(select_candidates(self.wines,'venue_a',Preferences()).wines[0].restaurant_id,'venue_a')

    def test_p15_08_postgres_positive_probes_read_back_all_synthetic_rows(self):
        cur=PositiveProbeCursor()
        _assert_database_positive_controls(cur)
        readbacks=[sql for sql,_ in cur.calls if sql.lstrip().startswith('SELECT wine_name,pronunciation')]
        self.assertEqual(len(readbacks),32)
        self.assertEqual(len(cur.values),16)
        self.assertEqual(cur.calls[-1][0],'RELEASE SAVEPOINT palate_positive_audit')

    def test_p15_08_postgres_probe_rejects_insert_and_update_rewrites(self):
        for change in (lambda row:('Tampered',*row[1:]),
                       lambda row:(*row[:-2],False,False)):
            cursor=PositiveProbeCursor(mutate=change)
            with self.subTest(change=change),self.assertRaisesRegex(RuntimeError,'silently rewritten'):
                _assert_database_positive_controls(cursor)
            self.assertEqual(cursor.calls[-2][0],'ROLLBACK TO SAVEPOINT palate_positive_audit')

    def test_p15_08_metadata_requires_both_v16_constraints(self):
        columns,constraints,indexes=valid_schema()
        _validate_schema_metadata(columns,constraints,indexes)
        for missing in ('wines_visible_text_contract_v16','wine_review_archive_visible_text_contract_v16'):
            with self.subTest(missing=missing),self.assertRaisesRegex(RuntimeError,'v16 display-safe'):
                _validate_schema_metadata(columns,[c for c in constraints if c[1]!=missing],indexes)

    def test_unicode_sql_newer_cf_codepoints_rejected_on_old_python(self):
        from pipeline import DEFAULT_IGNORABLE_POINTS, valid_required_wine_text
        # Python 3.10 labels these as unassigned, while the immutable SQL
        # migration prohibits them on every supported interpreter.
        stable_added=(0x0890,0x0891,*range(0x13439,0x13440))
        self.assertTrue(set(stable_added).issubset(DEFAULT_IGNORABLE_POINTS))
        for cp in stable_added:
            with self.subTest(codepoint=cp):
                self.assertFalse(valid_required_wine_text('Wine'+chr(cp)))

    def test_unicode_sql_forbidden_codepoint_list_matches_python(self):
        sql=(ROOT/'examples/migrations/008_visible_display_text.sql').read_text()
        found=re.search(r'ARRAY\[([0-9, ]+)\]::integer\[\]',sql)
        self.assertIsNotNone(found)
        got={int(x) for x in found.group(1).split(',')}
        from pipeline import DEFAULT_IGNORABLE_POINTS,DEFAULT_IGNORABLE_RANGES
        expected={cp for cp in range(0x110000) if unicodedata.category(chr(cp))=='Cf'}
        expected.update(DEFAULT_IGNORABLE_POINTS)
        expected.update(cp for a,b in DEFAULT_IGNORABLE_RANGES for cp in range(a,b+1))
        self.assertEqual(got,expected)

if __name__=='__main__':unittest.main()
