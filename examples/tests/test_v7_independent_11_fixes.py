"""Reproductions for the 11 independent v6 audit findings, using only synthetic data."""
from __future__ import annotations
from claim_fixture import claim_nonce
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import types
import unittest
from unittest import mock
import zipfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'examples'))
sys.path.insert(0,str(ROOT/'scripts'))

import acceptance_gate
import release_gate
import dbutil
import delivery
import maintenance
import postgres_catalog
import postgres_cli
import runtime
import worker
import wsgi
from pipeline import Preferences
from webhook import FormBinding
from webapp import WebhookApp
from delivery import EventStore, fingerprint_for


class V7IndependentRepairs(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.root=Path(t.name)

    def archive(self,entries):
        p=self.root/'candidate.zip'
        with zipfile.ZipFile(p,'w') as z:
            for name,data in entries:
                z.writestr('restaurant-wine-recommendation-reference/'+name,data)
        return p

    def test_01_archive_aliases_rejected_before_extraction(self):
        archive=self.archive([('docs/cache.txt','token-safe-placeholder'),
            ('docs/./cache.txt','credential-value-that-is-not-to-be-shipped')])
        self.assertTrue(any('noncanonical' in err for err in release_gate.scan_distribution(archive)))
        archive=self.archive([('docs/cache.txt','innocent'),('DOCS/CACHE.TXT','innocent2')])
        self.assertTrue(any('normalized' in err for err in release_gate.scan_distribution(archive)))
        archive=self.archive([('docs/cache.txt','innocent'),('docs//cache.txt','innocent2')])
        self.assertTrue(any('noncanonical' in err for err in release_gate.scan_distribution(archive)))

    def test_02_provider_secret_formats_detected_in_distributable(self):
        access='AK'+'IA'+('Z'*16)
        stripe='sk_'+'test_'+'W'*25
        secret='AWS_'+'SECRET_ACCESS_KEY='+('K'*40)
        a=self.archive([('docs/example.txt',f'\naccess={access}\n{secret}\nstripe={stripe}\n')])
        issues=release_gate.scan_distribution(a)
        self.assertTrue(any('credential' in e for e in issues),issues)
        safe=self.archive([('docs/example.txt','AWS_SECRET_ACCESS_KEY=replace_me\n')])
        self.assertEqual([],release_gate.scan_distribution(safe))

    def test_03_uncertain_delivery_never_requeued_from_free_text(self):
        event=EventStore(self.root/'uncertain.sqlite')
        self.addCleanup(event.close)
        pref=Preferences(category='red',sweetness='dry')
        token='uncertain-token'
        event.enqueue_request(token=token,venue='venue_a',recipient='client@example.test',preferences=pref,now=1)
        claim=event.claim(token,fingerprint_for('venue_a','client@example.test',pref),now=1)
        event.mark_dispatch_started(token,claim.attempt,now=2,nonce=claim_nonce(event,token,claim.attempt))
        event.mark_uncertain(token,claim.attempt,now=3,nonce=claim_nonce(event,token,claim.attempt))
        with self.assertRaisesRegex(ValueError,'provider submission may have been accepted'):
            event.reconcile(token,now=4,decision='confirmed_not_sent',evidence='I searched the provider but found no matching message')
        self.assertEqual(event.summary(token)[0],'reconciliation_required')
        self.assertEqual(event.claim(token,fingerprint_for('venue_a','client@example.test',pref),now=40).status,
                         'reconciliation_required')

    def test_03_never_submitted_retry_exhaustion_can_be_requeued(self):
        event=EventStore(self.root/'presend.sqlite')
        self.addCleanup(event.close)
        pref=Preferences(category='red',sweetness='dry')
        event.enqueue_request(token='safe',venue='venue_a',recipient='client@example.test',preferences=pref,now=1)
        fingerprint=fingerprint_for('venue_a','client@example.test',pref)
        for instant in (1,40,110):
            claim=event.claim('safe',fingerprint,now=instant)
            self.assertEqual(claim.status,'claimed')
            event.mark_retryable_failure('safe',claim.attempt,now=instant,nonce=claim_nonce(event,'safe',claim.attempt))
        self.assertEqual(event.summary('safe')[0],'retry_exhausted')
        event.reconcile('safe',now=120,decision='confirmed_not_sent',evidence='Pre-submission failure; no email send attempted')
        self.assertEqual(event.summary('safe')[0],'pending')

    def test_04_postgres_connection_enforces_verified_tls(self):
        connector=mock.Mock(return_value=object())
        fake=types.SimpleNamespace(connect=connector)
        with mock.patch.dict(sys.modules,{'psycopg2':fake}):
            factory=postgres_catalog.connect_from_env(dsn='host=database.example.test dbname=demo sslmode=disable')
            factory()
        args,kwargs=connector.call_args
        self.assertEqual(kwargs['sslmode'],'verify-full')
        self.assertEqual(kwargs['gssencmode'],'disable')
        self.assertEqual(kwargs['connect_timeout'],10)
        self.assertIn('sslmode=disable',args[0])

    def test_05_empty_venue_exists_independently_of_inventory(self):
        class FakeCursor:
            def __enter__(self):return self
            def __exit__(self,*exc):return False
            def execute(self,query,params):self.sql=query
            def fetchone(self):return (1,) if 'portfolio_v2.venues' in self.sql else None
            def fetchall(self):return []
        class FakeConn:
            def cursor(self):return FakeCursor()
            def close(self):pass
        store=postgres_catalog.PostgresCatalogStore(lambda:FakeConn())
        self.assertTrue(store.has_venue('empty-venue'))
        self.assertFalse(store.has_minimum_inventory('empty-venue'))

    def test_05_empty_registered_postgres_venue_sends_truthful_notice(self):
        class EmptyPg:
            def has_venue(self,venue):return True
            def wines_for_venue(self,venue):return ()
        store=EventStore(self.root/'empty-venue.sqlite')
        self.addCleanup(store.close)
        sent=[]
        def record_notice(recommendations):
            sent.append(recommendations)
            return 'accepted-notice'
        result=delivery.process_event(store,token='no-stock',claimed_venue='venue_a',
            authenticated_venue='venue_a',recipient_key='guest@example.test',
            preferences=Preferences(),sender=record_notice,now=10,
            catalog_store=EmptyPg(),notify_insufficient=True)
        self.assertEqual(result,'provider_accepted')
        self.assertEqual(sent,[[]])

    def test_06_quota_identity_is_case_insensitive_but_email_is_preserved(self):
        event=EventStore(self.root/'quotas.sqlite')
        self.addCleanup(event.close)
        prefs=Preferences()
        with mock.patch.object(delivery,'MAX_PER_RECIPIENT_DAY',2):
            self.assertEqual(event.enqueue_request(token='a',venue='venue_a',recipient='Ryan@Example.TEST',preferences=prefs,now=10),'queued')
            self.assertEqual(event.enqueue_request(token='b',venue='venue_a',recipient='ryan@example.test',preferences=prefs,now=11),'queued')
            self.assertEqual(event.enqueue_request(token='c',venue='venue_a',recipient='RYAN@EXAMPLE.TEST',preferences=prefs,now=12),'rate_limited')
        with event._connect() as conn:
            address=json.loads(conn.execute("SELECT request_json FROM events WHERE token='a'").fetchone()[0])['recipient']
        self.assertEqual(address,'Ryan@example.test')

    def test_07_webhook_catches_database_integrity_error(self):
        event=EventStore(self.root/'web.sqlite')
        self.addCleanup(event.close)
        app=WebhookApp(event,{'a':FormBinding('f','venue_a',b'synthetic-secret-key-for-tests')},clock=lambda:5)
        parsed=types.SimpleNamespace(token='t',venue_id='venue_a',recipient='a@example.test',preferences=Preferences())
        environ={'REQUEST_METHOD':'POST','PATH_INFO':'/webhook/a','CONTENT_TYPE':'application/json',
                 'CONTENT_LENGTH':'2','wsgi.input':io.BytesIO(b'{}')}
        replies=[]
        with mock.patch('webapp.verify_and_parse',return_value=parsed),mock.patch.object(event,'enqueue_request',side_effect=ValueError('State database replaced')):
            result=app(environ,lambda status,headers:replies.append(status))
        self.assertEqual(replies[0],'503 Service Unavailable')
        self.assertNotIn(b'replaced',b''.join(result))

    @unittest.skipUnless(os.name=='posix','Filesystem identity requires POSIX')
    def test_08_database_generation_blocks_inode_reuse(self):
        db=dbutil.SQLiteDatabase(self.root/'state.sqlite')
        self.addCleanup(db.close)
        with db.connect() as conn:conn.execute('CREATE TABLE expected(value TEXT)')
        other=self.root/'unrelated.sqlite'
        with contextlib.closing(sqlite3.connect(other)) as conn:
            with conn:conn.execute('CREATE TABLE decoy(value TEXT)')
        primary=Path(db.path)
        primary.rename(self.root/'old.sqlite')
        shutil.copy2(other,primary)
        primary.chmod(0o600)  # Keep confidentiality intact; isolate generation detection.
        # Simulate filesystem inode reuse: old (dev,inode) check succeeds.
        stat=os.stat(primary)
        db._state_identity=(stat.st_dev,stat.st_ino)
        with self.assertRaisesRegex(ValueError,'generation'):
            with db.connect() as conn:conn.execute('SELECT * FROM decoy')

    def test_09_archive_digest_streamed_and_size_bounded(self):
        archive=self.archive([('README.md','safe')])
        with mock.patch.object(Path,'read_bytes',side_effect=AssertionError('No unbounded read_bytes')):
            with mock.patch.object(acceptance_gate,'scan_distribution',return_value=['synthetic_block']):
                result=acceptance_gate.verify(archive)
        self.assertIn('unsafe artifact',result['decision'])
        huge=self.root/'huge.zip'
        with huge.open('wb') as f:f.truncate(20_000_001)
        with self.assertRaisesRegex(ValueError,'maximum compressed size'):
            acceptance_gate.verify(huge)

    def test_10_operational_main_entry_points_are_exercised(self):
        events=mock.Mock()
        events.unresolved.return_value=[('id','retry_exhausted',3,'test',2)]
        events.history.return_value=[(1,'pending',2,'queued')]
        events.purge_terminal.return_value=1
        with mock.patch.object(maintenance,'EventStore',return_value=events), mock.patch.dict(os.environ,{'EVENT_DB_PATH':'/tmp/synthetic-reference.sqlite'}):
            for args in (['unresolved'],['history','one'],['purge'],['reconcile','one','cancel','--evidence','Manually closed example reference']):
                with self.subTest(args=args),mock.patch.object(sys,'argv',['maintenance.py',*args]),contextlib.redirect_stdout(io.StringIO()):
                    maintenance.main()
        self.assertEqual(events.close.call_count,4)
        pg=mock.Mock()
        pg.import_legacy.return_value={'legacy-1':2}
        with mock.patch.object(postgres_cli,'PostgresCatalogStore',return_value=pg),mock.patch.object(postgres_cli,'connect_from_env',return_value=mock.Mock()):
            for args in (['init'],['import-legacy'],['bind-alias','--venue','v','--wine-id','w','--name','Wine'],
                         ['approve-pairings','--venue','v','--wine-id','w','--tags','fish']):
                with self.subTest(args=args),mock.patch.object(sys,'argv',['postgres_cli.py',*args]),contextlib.redirect_stdout(io.StringIO()):
                    postgres_cli.main()
        # Run worker --once without spawning or contacting any provider.
        worker_store=mock.Mock();worker_store.alerts.return_value=[]
        comps=(worker_store,mock.Mock(),mock.Mock(),mock.Mock())
        with mock.patch.object(worker,'create_worker_components',return_value=comps),\
             mock.patch.object(worker,'run_due',return_value={'processed':{},'uncertain_submissions_held':0}),\
             mock.patch.object(sys,'argv',['worker.py','--once']),contextlib.redirect_stdout(io.StringIO()):
            worker.main()
        worker_store.close.assert_called_once()
        sentinel=object()
        with mock.patch.object(wsgi,'create_http_app',return_value=sentinel):
            self.assertIs(wsgi.get_application(),sentinel)

    def test_12_current_findings_are_pinned_in_historical_anchor(self):
        current=json.loads((ROOT/'quality/issue_register.json').read_text())
        baseline=(ROOT/'quality/issue_history_baseline.json').read_bytes()
        self.assertEqual([],acceptance_gate.check_historical_issue_inventory(current,baseline))
        changed={**current,'findings':[row for row in current['findings'] if row['id']!='V6-03']}
        failures=acceptance_gate.check_historical_issue_inventory(changed,baseline)
        self.assertTrue(any('V6-03' in x for x in failures))

    def test_11_readme_documents_current_release(self):
        readme=(ROOT/'README.md').read_text()
        self.assertIn('v7 release controls',readme)
        self.assertIn('v7-security-remediation.md',readme)
        self.assertNotIn('v5 repair verification](docs/v5-remediation-status.md) for the latest',readme)

if __name__=='__main__': unittest.main()
