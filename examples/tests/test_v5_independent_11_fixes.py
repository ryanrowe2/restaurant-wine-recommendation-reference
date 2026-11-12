"""Fresh adversarial regressions for the v4 eleven-finding prepublication audit.

No production network calls or credentials are used.
"""
from __future__ import annotations
from claim_fixture import claim_nonce
import base64
import hashlib
import hmac
import io
import json
import tempfile
import sys
import unittest
from pathlib import Path
from unittest import mock

EXAMPLES=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(EXAMPLES))
from catalog_store import CatalogStore
from delivery import (EventStore, PermanentProviderReject, fingerprint_for,
                      process_event)
from pipeline import MAX_CATALOG_ROWS, Preferences, load_catalog
from postgres_catalog import PostgresCatalogStore, extract_reviewable_pairings
from providers import MODEL_OUTPUT_SCHEMA, StructuredRecommender,PostmarkSender,PostmarkReceiptVerifier
from runtime import _configured_catalog,create_http_app,create_worker_components
from webhook import FormBinding
from webapp import WebhookApp
from test_v3_postgres_contract import FakePG


class NewIndependentRegressions(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root=Path(temp.name)
        self.prefs=Preferences(category='red',sweetness='dry')
        self.wine=next(w for w in load_catalog() if w.restaurant_id=='venue_a')

    def store(self):
        store=EventStore(self.root/'events.sqlite3')
        self.addCleanup(store.close)
        return store

    def test_01_approved_pairings_survive_replacement(self):
        prior=(self.wine.wine_id,'source tag','source descriptors','source body',
               'source dishes',['red_meat'],True,['red_meat'])
        pg=FakePG(provenance=(prior,))
        PostgresCatalogStore(lambda:pg).replace_venue('venue_a',[self.wine])
        params=[p for q,p in pg.queries if q.startswith('insert into portfolio_v2.wines')][0]
        self.assertEqual(json.loads(params[9]),['red_meat'])
        self.assertTrue(params[17])
        self.assertEqual(params[12:16],prior[1:5])

    def test_01_unreviewed_pairings_never_promoted(self):
        prior=(self.wine.wine_id,'metadata','descriptors','body','pairing',
               ['fish'],False,['fish'])
        pg=FakePG(provenance=(prior,))
        PostgresCatalogStore(lambda:pg).replace_venue('venue_a',[self.wine])
        params=[p for q,p in pg.queries if q.startswith('insert into portfolio_v2.wines')][0]
        self.assertFalse(params[17])
        self.assertEqual(json.loads(params[9]),[])
        self.assertEqual(json.loads(params[16]),['fish'])

    def test_02_direct_wsgi_rejects_shared_signing_secrets(self):
        s=self.store()
        shared=b'shared-signing-key-for-test'
        with self.assertRaisesRegex(ValueError,'signing key'):
            WebhookApp(s,{'a':FormBinding('a-form','venue_a',shared),
                          'b':FormBinding('b-form','venue_b',shared)})
        self.assertEqual(len(WebhookApp(s,{'a':FormBinding('a-form','venue_a',shared)}).bindings),1)

    def test_03_explicit_dsn_propagates_to_http_and_worker(self):
        env={'PG_DSN':'host=testing.invalid dbname=isolated',
             'EVENT_DB_PATH':str(self.root/'events.sqlite3'),
             'TYPEFORM_BINDINGS_JSON':json.dumps({'a':{'form_id':'fa','venue_id':'venue_a','secret_env':'TYPEFORM_SECRET_A'}}),
             'TYPEFORM_SECRET_A':'independent-twenty-byte-secret',
             'OPENAI_API_KEY':'fake-no-network','POSTMARK_SERVER_TOKEN':'fake-no-network',
             'SENDER_EMAIL':'sender@example.test'}
        with mock.patch('postgres_catalog.connect_from_env',return_value=lambda:None) as patched:
            app=create_http_app(environ=env)
            self.assertIsInstance(app,WebhookApp)
            app.store.close()
            worker=create_worker_components(environ=env)
            worker[0].close()
            self.assertEqual(patched.call_count,2)
            for call in patched.call_args_list:
                self.assertEqual(call.kwargs['dsn'],env['PG_DSN'])

    def test_04_operator_cannot_confirm_only_known_rejections(self):
        store=self.store()
        store.enqueue_request(token='rejected',venue='venue_a',recipient='r@example.test',preferences=self.prefs,now=1)
        fp=fingerprint_for('venue_a','r@example.test',self.prefs)
        first=store.claim('rejected',fp,now=1)
        store.mark_dispatch_started('rejected',first.attempt,now=2,nonce=claim_nonce(store,'rejected',first.attempt))
        store.mark_known_not_accepted('rejected',first.attempt,now=2,nonce=claim_nonce(store,'rejected',first.attempt))
        for t in (33,94):
            c=store.claim('rejected',fp,now=t)
            store.mark_retryable_failure('rejected',c.attempt,now=t,nonce=claim_nonce(store,'rejected',c.attempt))
        self.assertEqual(store.summary('rejected')[0],'retry_exhausted')
        with self.assertRaisesRegex(ValueError,'no submission'):
            store.reconcile('rejected',decision='confirmed_accepted',evidence='Provider dashboard lookup confirms receipt',
                            provider_ref='made-up',now=100)

    def test_04_current_unresolved_submission_still_reconcilable(self):
        store=self.store()
        store.enqueue_request(token='uncertain',venue='venue_a',recipient='r@example.test',preferences=self.prefs,now=1)
        fp=fingerprint_for('venue_a','r@example.test',self.prefs)
        claim=store.claim('uncertain',fp,now=1)
        store.mark_dispatch_started('uncertain',claim.attempt,now=2,nonce=claim_nonce(store,'uncertain',claim.attempt))
        store.mark_uncertain('uncertain',claim.attempt,now=3,nonce=claim_nonce(store,'uncertain',claim.attempt))
        store.reconcile('uncertain',decision='confirmed_accepted',evidence='Verified this reference from provider dashboard',
                        provider_ref='verified-123',receipt_verifier=lambda ref,rec,hash_value: bool(ref and rec and hash_value),now=4)
        self.assertEqual(store.summary('uncertain')[0],'provider_accepted')

    def test_05_existing_alias_is_promoted_to_reviewed(self):
        pg=FakePG(aliases=(('old label','stable-id'),))
        PostgresCatalogStore(lambda:pg).bind_legacy_name('venue_a','Old Label','stable-id')
        upserts=[q for q,p in pg.queries if q.startswith('insert into portfolio_v2.legacy_identity_map')]
        self.assertEqual(len(upserts),1)
        self.assertIn('do update set reviewed=true',upserts[0])
        self.assertIn('where portfolio_v2.legacy_identity_map.wine_id=excluded.wine_id',upserts[0])

    def test_06_permanent_model_outage_is_audited_and_alerted(self):
        event=self.store()
        catalog=CatalogStore(self.root/'catalog.db');self.addCleanup(catalog.close)
        catalog.replace_venue('venue_a',[w for w in load_catalog() if w.restaurant_id=='venue_a'])
        sent=[]
        def unavailable(*args):
            raise PermanentProviderReject('SENSITIVE ORIGINAL ERROR STRING')
        result=process_event(event,token='model-failure',claimed_venue='venue_a',authenticated_venue='venue_a',
            recipient_key='r@example.test',preferences=self.prefs,catalog_store=catalog,
            recommender=unavailable,sender=lambda recs:sent.append(recs) or 'accepted',now=100)
        self.assertEqual(result,'provider_accepted')
        self.assertEqual(len(sent),1)
        codes={a['code'] for a in event.alerts(now=200)}
        self.assertIn('model_configuration_degraded',codes)
        self.assertTrue(any(row[3]=='model_degraded:configuration' for row in event.history('model-failure')))
        self.assertNotIn('SENSITIVE',str(event.history('model-failure')))

    def test_07_sqlite_readiness_does_not_fetch_full_catalog(self):
        catalog=CatalogStore(self.root/'catalog.db');self.addCleanup(catalog.close)
        wines=[w for w in load_catalog() if w.restaurant_id=='venue_a']
        catalog.replace_venue('venue_a',wines)
        self.assertTrue(catalog.has_minimum_inventory('venue_a'))
        self.assertFalse(catalog.has_minimum_inventory('venue_a',len(wines)+1))
        with mock.patch.object(catalog,'wines_for_venue',side_effect=AssertionError('FULL READ')):
            store=self.store();store.worker_heartbeat(now=100)
            app=WebhookApp(store,{'a':FormBinding('form','venue_a',b'valid-signing-secret')},clock=lambda:101,catalog=catalog)
            states=[]
            app({'REQUEST_METHOD':'GET','PATH_INFO':'/health'},lambda status,headers:states.append(status))
            self.assertEqual(states,['200 OK'])

    def test_07_postgres_readiness_query_bounded(self):
        pg=FakePG()
        PostgresCatalogStore(lambda:pg).has_minimum_inventory('venue_a',3)
        queries=[q for q,p in pg.queries]
        self.assertTrue(any('limit %s' in q and 'select 1 from portfolio_v2.wines' in q for q in queries))
        self.assertFalse(any('select venue_id,wine_id' in q for q in queries))

    def test_08_sqlite_iterator_limit_stops_at_max_plus_one(self):
        catalog=CatalogStore(self.root/'catalog.db');self.addCleanup(catalog.close)
        emitted=[]
        def source():
            for i in range(MAX_CATALOG_ROWS+1):
                emitted.append(1)
                yield self.wine
            raise AssertionError('source overconsumed')
        with self.assertRaises(ValueError):catalog.replace_venue('venue_a',source())
        self.assertEqual(len(emitted),MAX_CATALOG_ROWS+1)

    def test_08_postgres_iterator_limit_stops_at_max_plus_one(self):
        emitted=[]
        def source():
            for i in range(MAX_CATALOG_ROWS+1):
                emitted.append(1)
                yield self.wine
            raise AssertionError('source overconsumed')
        with self.assertRaises(ValueError):PostgresCatalogStore(lambda:None).replace_venue('venue_a',source())
        self.assertEqual(len(emitted),MAX_CATALOG_ROWS+1)

    def test_09_recipient_quota_does_not_charge_idempotent_redelivery(self):
        store=self.store()
        with mock.patch('delivery.MAX_PER_RECIPIENT_DAY',2):
            self.assertEqual(store.enqueue_request(token='t1',venue='venue_a',recipient='r@example.test',preferences=self.prefs,now=100),'queued')
            self.assertEqual(store.enqueue_request(token='t1',venue='venue_a',recipient='r@example.test',preferences=self.prefs,now=101),'already_queued')
            self.assertEqual(store.enqueue_request(token='t2',venue='venue_b',recipient='r@example.test',preferences=self.prefs,now=101),'queued')
            self.assertEqual(store.enqueue_request(token='t3',venue='venue_a',recipient='r@example.test',preferences=self.prefs,now=102),'rate_limited')
            self.assertIsNone(store.summary('t3'))
            self.assertIn('admission_quota_exceeded',{x['code'] for x in store.alerts(now=103)})
            self.assertEqual(store.enqueue_request(token='t3',venue='venue_a',recipient='r@example.test',preferences=self.prefs,now=86400+103),'queued')

    def test_09_venue_quota_limits_distinct_recipients(self):
        store=self.store()
        with mock.patch('delivery.MAX_PER_VENUE_HOUR',2),mock.patch('delivery.MAX_PER_RECIPIENT_DAY',10):
            for i in (1,2):
                self.assertEqual(store.enqueue_request(token=f't{i}',venue='venue_a',recipient=f'r{i}@example.test',preferences=self.prefs,now=100),'queued')
            self.assertEqual(store.enqueue_request(token='t3',venue='venue_a',recipient='r3@example.test',preferences=self.prefs,now=101),'rate_limited')

    def test_09_wsgI_returns_explicit_rate_limit_response(self):
        store=self.store()
        secret=b'distinct-long-signing-secret'
        binding=FormBinding('f1','venue_a',secret)
        body=json.dumps({'event_type':'form_response','form_response':{'form_id':'f1','token':'t1','submitted_at':'1970-01-01T00:01:40Z',
            'hidden':{'restaurant_id':'venue_a'},'answers':[{'type':'email','email':'test@example.test'}],
            'variables':[{'key':'wine_category','type':'text','text':'red'},
                         {'key':'sweetness','type':'text','text':'dry'}]}}).encode()
        signature='sha256='+base64.b64encode(hmac.new(secret,body,hashlib.sha256).digest()).decode()
        app=WebhookApp(store,{'a':binding},clock=lambda:100)
        response=[]
        with mock.patch('delivery.MAX_PER_RECIPIENT_DAY',0):
            app({'REQUEST_METHOD':'POST','PATH_INFO':'/webhook/a','CONTENT_TYPE':'application/json',
                 'CONTENT_LENGTH':str(len(body)),'HTTP_TYPEFORM_SIGNATURE':signature,'wsgi.input':io.BytesIO(body)},
                lambda code,headers:response.append(code))
        self.assertTrue(response[0].startswith('429'))

    def test_10_negated_source_pairings_not_proposed(self):
        for phrase in ('Excellent with salmon, not with steak.','Pairs with fish, but not for beef.',
                       'Avoid chicken even when served with pasta.'):
            with self.subTest(phrase=phrase):
                self.assertFalse(extract_reviewable_pairings(phrase))

    def test_11_boolean_retention_rejected(self):
        store=self.store()
        for value in (True,False):
            with self.subTest(value=value),self.assertRaises(ValueError):
                store.purge_terminal(now=100000,retention_days=value)

    def test_strict_model_schema_limits_ids_with_supported_enum(self):
        selection=__import__('pipeline').select_candidates(load_catalog(),'venue_a',self.prefs,max_candidates=12)
        captured=[]
        class Answer:
            status=200
            def __enter__(self):return self
            def __exit__(self,*args):return False
            def read(self,n):
                return json.dumps({'choices':[{'message':{'content':json.dumps({'recommendations':
                    [{'wine_id':w.wine_id} for w in selection.wines[:3]]})}}]}).encode()
        def open_fn(req,timeout):
            captured.append(json.loads(req.data));return Answer()
        recs=StructuredRecommender('fake-token',opener=open_fn)(selection,self.prefs)
        self.assertEqual(len(recs),3)
        schema=captured[0]['response_format']['json_schema']['schema']
        field=schema['properties']['recommendations']['items']['properties']['wine_id']
        self.assertEqual(set(field['enum']),{w.wine_id for w in selection.wines})
        self.assertNotIn('maxLength',field)
        self.assertEqual((schema['properties']['recommendations']['minItems'],schema['properties']['recommendations']['maxItems']),(3,3))

    def test_receipt_verifier_requires_matching_provider_metadata_and_recipient(self):
        token_hash=hashlib.sha256(b'uncertain-token').hexdigest()
        details={'MessageID':'pm-msg-123','Status':'Processed','From':'sender@example.test',
                 'Recipients':['dest@example.test'],'Metadata':{'PalateEvent':token_hash}}
        captured=[]
        class Answer:
            status=200
            def __enter__(self):return self
            def __exit__(self,*args):return False
            def read(self,n):return json.dumps(details).encode()
        def open_fn(req,timeout):
            captured.append(req)
            return Answer()
        verify=PostmarkReceiptVerifier('mock-server-token','sender@example.test',opener=open_fn)
        self.assertTrue(verify('pm-msg-123','dest@example.test',token_hash))
        self.assertEqual(captured[0].get_method(),'GET')
        self.assertEqual(captured[0].full_url,'https://api.postmarkapp.com/messages/outbound/pm-msg-123/details')
        self.assertFalse(verify('pm-msg-123','other@example.test',token_hash))
        details['Metadata']={'PalateEvent':'0'*64}
        self.assertFalse(verify('pm-msg-123','dest@example.test',token_hash))
        details['Metadata']={'PalateEvent':token_hash}
        details['Status']='Bounced'
        self.assertFalse(verify('pm-msg-123','dest@example.test',token_hash))
        self.assertFalse(verify('../attacker','dest@example.test',token_hash))

    def test_postmark_sender_passes_only_hashed_event_metadata(self):
        token_hash=hashlib.sha256(b'fictional-token').hexdigest()
        posted=[]
        class Answer:
            status=200
            def __enter__(self):return self
            def __exit__(self,*args):return False
            def read(self,n):return b'{"ErrorCode":0,"MessageID":"msg-ref","To":"dest@example.test"}'
        def open_fn(req,timeout):
            posted.append(json.loads(req.data));return Answer()
        sender=PostmarkSender('synthetic','sender@example.test',opener=open_fn)
        self.assertEqual(sender.send('dest@example.test',[],token_hash=token_hash),'msg-ref')
        self.assertEqual(posted[0]['Metadata'],{'PalateEvent':token_hash})
        self.assertNotIn('fictional-token',str(posted))
        with self.assertRaises(ValueError):sender.send('dest@example.test',[],token_hash='bad')

    def test_operator_acceptance_rejected_without_verifier(self):
        store=self.store()
        store.enqueue_request(token='no-proof',venue='venue_a',recipient='r@example.test',preferences=self.prefs,now=1)
        fp=fingerprint_for('venue_a','r@example.test',self.prefs)
        claim=store.claim('no-proof',fp,now=1)
        store.mark_dispatch_started('no-proof',claim.attempt,now=2,nonce=claim_nonce(store,'no-proof',claim.attempt))
        store.mark_uncertain('no-proof',claim.attempt,now=3,nonce=claim_nonce(store,'no-proof',claim.attempt))
        with self.assertRaisesRegex(ValueError,'verifier'):
            store.reconcile('no-proof',now=4,decision='confirmed_accepted',
                            evidence='Confirmed acceptance in provider records',provider_ref='pm-msg-123')
        self.assertEqual(store.summary('no-proof')[0],'reconciliation_required')
        with self.assertRaisesRegex(ValueError,'verification'):
            store.reconcile('no-proof',now=4,decision='confirmed_accepted',
                            evidence='Confirmed acceptance in provider records',provider_ref='pm-msg-123',
                            receipt_verifier=lambda *args:False)
        self.assertEqual(store.summary('no-proof')[0],'reconciliation_required')

    def test_provider_verification_rechecks_event_after_network_lookup(self):
        store=self.store()
        store.enqueue_request(token='race',venue='venue_a',recipient='r@example.test',preferences=self.prefs,now=1)
        fp=fingerprint_for('venue_a','r@example.test',self.prefs)
        claim=store.claim('race',fp,now=1)
        store.mark_dispatch_started('race',claim.attempt,now=2,nonce=claim_nonce(store,'race',claim.attempt))
        store.mark_uncertain('race',claim.attempt,now=3,nonce=claim_nonce(store,'race',claim.attempt))
        def changed(*args):
            store.reconcile('race',now=4,decision='cancel',evidence='Operator independently cancelled delivery')
            return True
        with self.assertRaisesRegex(ValueError,'eligible'):
            store.reconcile('race',now=4,decision='confirmed_accepted',
                            evidence='Provider receipt independently verified',provider_ref='pm-msg-123',receipt_verifier=changed)
        self.assertEqual(store.summary('race')[0],'reconciled_cancelled')

    def test_quota_denials_are_aggregated_not_unbounded_rows(self):
        store=self.store()
        with mock.patch('delivery.MAX_PER_VENUE_HOUR',0):
            for i in range(25):
                outcome=store.enqueue_request(token=f'new-{i}',venue='venue_a',recipient=f'different{i}@example.test',preferences=self.prefs,now=100)
                self.assertEqual(outcome,'rate_limited')
        with store._connect() as conn:
            rows=conn.execute('SELECT venue,reason,denied_count FROM admission_denials').fetchall()
        self.assertEqual(rows,[('venue_a','venue_hour',25)])

if __name__=='__main__':unittest.main()
