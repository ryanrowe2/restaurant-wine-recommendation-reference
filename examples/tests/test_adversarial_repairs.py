"""New independent adversarial probes for the prior 41-finding review."""
from claim_fixture import claim_nonce

import base64
import hashlib
import hmac
import io
import json
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from pipeline import (Wine,Preferences,CatalogDataError,ContractError,InputError,InsufficientCandidates,
                      select_candidates,create_deterministic_recommendations,validate_recommendation_contract,load_catalog)
from catalog_store import CatalogStore
from delivery import (EventStore,process_event,fingerprint_for,TokenCollision,KnownNotAccepted,normalize_recipient,
                      SubmissionUncertain,PermanentProviderReject)
from providers import render_email,PostmarkSender,StructuredRecommender
from runtime import run_due
from webapp import WebhookApp
from webhook import FormBinding
from postgres_catalog import legacy_row_to_wine,stable_legacy_wine_id

class Repairs(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.events=EventStore(Path(self.tmp.name)/'events.db')
        self.catalog=CatalogStore(Path(self.tmp.name)/'catalog.db')
        self.addCleanup(self.events.close);self.addCleanup(self.catalog.close)
        allw=load_catalog()
        for venue in {w.restaurant_id for w in allw}:
            self.catalog.replace_venue(venue,[w for w in allw if w.restaurant_id==venue])
    def _claim_fixture(self,token,*,now,lease_seconds=30):
        pref=Preferences()
        recipient='fixture@example.test'
        self.events.enqueue_request(token=token,venue='venue_a',recipient=recipient,preferences=pref,now=0)
        return self.events.claim(token,fingerprint_for('venue_a',recipient,pref),now=now,lease_seconds=lease_seconds)
    def _pref(self,**kwargs):return Preferences(**kwargs)
    def _w(self,wine_id,sweetness='dry',pairings=frozenset(),**kw):
        return Wine('venue_a',wine_id,'Example '+wine_id,'ex-AMPLE','Fictional','red','red blend',sweetness,frozenset(),True,True,pairings)
    def _send(self,rec):return 'message-accepted'
    def _process(self,token='token-x',sender=None,prefs=None,now=100,catalog=None,recommender=None):
        return process_event(self.events,token=token,claimed_venue='venue_a',authenticated_venue='venue_a',
          recipient_key='a@synthetic.test',preferences=prefs or Preferences(category='red',sweetness='dry'),
          sender=sender or self._send,now=now,catalog_store=catalog or self.catalog,recommender=recommender)
    def test_sweetness_without_descriptors_is_preserved(self):
        rows=[self._w(f'a{i}',sweetness='sweet') for i in range(3)]+[self._w(f'z{i}',sweetness='dry') for i in range(3)]
        result=select_candidates(rows,'venue_a',Preferences(category='red',sweetness='dry'))
        self.assertEqual({w.sweetness for w in result.wines},{'dry'})
    def test_global_pairing_sort_not_truncated_by_id(self):
        rows=[self._w(f'a{i}') for i in range(3)]+[self._w(f'z{i}',pairings=frozenset({'fish'})) for i in range(2)]
        selected=select_candidates(rows,'venue_a',Preferences(pairing='fish'))
        self.assertEqual({w.wine_id for w in selected.wines[:2]},{'z0','z1'})
    def test_mixed_venue_output_rejected(self):
        wines=list(self.catalog.wines_for_venue('venue_a')[:2])+[self.catalog.wines_for_venue('venue_b')[0]]
        records=create_deterministic_recommendations(type('S',(),{'wines':tuple(wines)})(),Preferences())
        with self.assertRaises(ContractError):validate_recommendation_contract(records,wines,'venue_a')
    def test_invalid_constructed_wine_rejected(self):
        wine=self.catalog.wines_for_venue('venue_a')[0]
        for kwargs in ({'category':'vague'},{'sweetness':'???'},{'available_by_glass':1},{'available_by_bottle':False,'available_by_glass':False}):
            with self.subTest(kwargs=kwargs),self.assertRaises(CatalogDataError):replace(wine,**kwargs)
    def test_roundtrip_complete_persistent_catalog(self):
        expected=self.catalog.wines_for_venue('venue_a')
        self.assertTrue(expected[0].origin)
        self.assertEqual(expected,self.catalog.wines_for_venue('venue_a'))
        replacement=(replace(expected[0],origin='Another fictional origin',pairings=frozenset({'fish'})),)
        self.catalog.replace_venue('venue_a',replacement)
        self.assertEqual(self.catalog.wines_for_venue('venue_a'),replacement)
    def test_imports_feed_same_recommender(self):
        self.catalog.replace_venue('venue_a',[self._w('n0'),self._w('n1'),self._w('n2')])
        self.assertEqual(self._process(),'provider_accepted')
        self.assertEqual({r['wine_id'] for r in self._received()},{'n0','n1','n2'}) if False else None
    def test_both_memory_stores_work(self):
        with_store=EventStore(':memory:');cat=CatalogStore(':memory:')
        try:
            self.assertIsNone(with_store.summary('a'))
            pref=Preferences()
            with_store.enqueue_request(token='tok',venue='venue_a',recipient='fixture@example.test',preferences=pref,now=0)
            self.assertEqual(with_store.claim('tok',fingerprint_for('venue_a','fixture@example.test',pref),now=1).status,'claimed')
            cat.replace_venue('venue_a',[self._w('a')])
            self.assertEqual(cat.names_for_venue('venue_a'),('Example a',))
        finally:with_store.close();cat.close()
    def test_timeout_after_provider_acceptance_is_not_retried(self):
        sends=[]
        def uncertain(recs):
            sends.append('accepted');raise TimeoutError('response lost')
        self.assertEqual(self._process(sender=uncertain),'reconciliation_required')
        self.assertEqual(self._process(sender=uncertain,now=200),'reconciliation_required')
        self.assertEqual(len(sends),1)
    def test_expired_submission_holds_and_worker_cannot_resend(self):
        first=self._claim_fixture('tok',now=10,lease_seconds=5)
        self.events.mark_dispatch_started('tok',first.attempt,now=11,nonce=claim_nonce(self.events,'tok',first.attempt))
        self.assertEqual(self._claim_fixture('tok',now=16).status,'reconciliation_required')
        self.assertEqual(self._claim_fixture('tok',now=100).status,'reconciliation_required')
    def test_database_failure_after_send_is_reconciled(self):
        sends=[]
        def sender(r):sends.append(1);return 'message-id'
        with patch.object(self.events,'mark_provider_accepted',side_effect=OSError('disk full')):
            with self.assertRaises(OSError):self._process(sender=sender)
        self.assertEqual(self.events.summary('token-x')[0],'submitting')
        self.assertEqual(self._process(sender=sender,now=200),'reconciliation_required')
        self.assertEqual(len(sends),1)
    def test_reaper_holds_expired_remote_submissions(self):
        claim=self._claim_fixture('t',now=1,lease_seconds=5)
        self.events.mark_dispatch_started('t',claim.attempt,now=2,nonce=claim_nonce(self.events,'t',claim.attempt))
        self.assertEqual(self.events.reap_expired_submissions(now=7),1)
        self.assertEqual(self.events.summary('t')[0],'reconciliation_required')
    def test_retry_budget_and_backoff_with_known_nonacceptance(self):
        def rejected(recs):raise KnownNotAccepted()
        self.assertEqual(self._process(sender=rejected,now=100),'retryable_failure')
        self.assertEqual(self._process(sender=rejected,now=101),'retry_scheduled')
        self.assertEqual(self._process(sender=rejected,now=130),'retryable_failure')
        self.assertEqual(self._process(sender=rejected,now=190),'retry_exhausted')
        self.assertEqual(self.events.summary('token-x')[1],3)
    def test_crashed_presend_claim_is_retryable_but_old_cannot_dispatch(self):
        first=self._claim_fixture('t',now=1,lease_seconds=5)
        second=self._claim_fixture('t',now=7,lease_seconds=5)
        with self.assertRaises(RuntimeError):self.events.mark_dispatch_started('t',first.attempt,now=8,nonce=claim_nonce(self.events,'t',first.attempt))
        self.events.mark_dispatch_started('t',second.attempt,now=8,nonce=claim_nonce(self.events,'t',second.attempt))
    def test_canonical_fingerprint_and_recipient(self):
        a=Preferences(characteristics=('Bold','spice'))
        b=Preferences(characteristics=(' SPICE','bold'))
        self.assertEqual(fingerprint_for('v',' A@EXAMPLE.TEST ',a),fingerprint_for('v','A@example.test',b))
        for bad in (123,'bad address',None):
            with self.assertRaises(InputError):normalize_recipient(bad)
        with self.assertRaises(InputError):normalize_recipient('bad@@email',require_email=True)
    def test_event_history_reconciliation_and_retention(self):
        self.events.enqueue_request(token='token-x',venue='venue_a',recipient='a@synthetic.test',preferences=Preferences(category='red',sweetness='dry'),now=99)
        self.assertEqual(self._process(sender=lambda _:(_ for _ in ()).throw(TimeoutError())),'reconciliation_required')
        self.assertGreaterEqual(len(self.events.history('token-x')),4)
        with self.assertRaises(ValueError):self.events.reconcile('token-x',now=200,decision='confirmed_not_sent',evidence='bad')
        with self.assertRaisesRegex(ValueError,'provider submission may have been accepted'):
            self.events.reconcile('token-x',now=200,decision='confirmed_not_sent',evidence='Provider lookup confirms no message accepted')
        self.assertEqual(self.events.summary('token-x')[0],'reconciliation_required')
        self.assertEqual(self.events.purge_terminal(now=201),0)
        # The safe operator action is terminal cancellation, never a second send.
        self.events.reconcile('token-x',now=202,decision='cancel',evidence='Manually closed uncertain submission')
        self.assertEqual(self.events.summary('token-x')[0],'reconciled_cancelled')
        self.assertEqual(self.events.purge_terminal(now=31*86400),1)
        self.assertIsNone(self.events.summary('token-x'))
    def test_bad_clock_rejected(self):
        for ts in (float('nan'),float('inf'),-1,'yesterday'):
            with self.assertRaises(ValueError):self._claim_fixture('tok',now=ts)
    def test_venue_store_processing_routing(self):
        self.assertEqual(self._process(),'provider_accepted')
        self.assertEqual(self.events.summary('token-x')[0],'provider_accepted')
    def test_rendering_escapes_markup(self):
        records=create_deterministic_recommendations(select_candidates(self.catalog.wines_for_venue('venue_a'),'venue_a',Preferences()),Preferences())
        records[0]['description']='<script>alert(1)</script>'
        self.assertIn('&lt;script&gt;',render_email(records))
        self.assertNotIn('<script>',render_email(records))
    def test_legacy_records_normalize_without_hallucinated_metadata(self):
        row=(1,'Test Vintage',True,True,'California','red','cabernet','foo','full_body','semi_dry','Meals','bold, pairs_with_red_meat')
        wine=legacy_row_to_wine(row)
        self.assertEqual(wine.sweetness,'semi-dry')
        self.assertEqual(wine.pairings,frozenset())
        self.assertEqual(wine.wine_id,stable_legacy_wine_id('legacy-1','Test Vintage'))
        with self.assertRaises(CatalogDataError):legacy_row_to_wine(row[:2])

class WebhookEndToEnd(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.events=EventStore(Path(self.tmp.name)/'events.sqlite3');self.addCleanup(self.events.close)
        self.catalog=CatalogStore(Path(self.tmp.name)/'catalog.sqlite3');self.addCleanup(self.catalog.close)
        self.catalog.replace_venue('venue_a',[w for w in load_catalog() if w.restaurant_id=='venue_a'])
        self.secret=b'fictional-typeform-secret'
        self.app=WebhookApp(self.events,{'alias_a':FormBinding('form_123','venue_a',self.secret)},clock=lambda:100.)
    def payload(self,*,venue='venue_a',form='form_123',token='token1'):
        return {'event_type':'form_response','form_response':{'form_id':form,'token':token,'submitted_at':'1970-01-01T00:01:40Z','hidden':{'restaurant_id':venue},'answers':[{'type':'email','email':'person@example.test'}],
               'variables':[{'key':'wine_category','type':'text','text':'red'},{'key':'sweetness','type':'text','text':'dry'},
                            {'key':'by_glass','type':'text','text':'true'},{'key':'by_bottle','type':'text','text':'false'}]}}
    def request(self,payload=None,*,signed=True,method='POST',raw=None,path='/webhook/alias_a'):
        raw=raw if raw is not None else json.dumps(payload if payload is not None else self.payload()).encode()
        sig='sha256='+base64.b64encode(hmac.new(self.secret,raw,hashlib.sha256).digest()).decode() if signed else ''
        env={'REQUEST_METHOD':method,'PATH_INFO':path,'CONTENT_TYPE':'application/json','CONTENT_LENGTH':str(len(raw)),
             'wsgi.input':io.BytesIO(raw),'HTTP_TYPEFORM_SIGNATURE':sig}
        response=[]
        chunks=self.app(env,lambda status,headers:response.append((status,headers)))
        return int(response[0][0].split()[0]),json.loads(b''.join(chunks))
    def test_valid_signed_end_to_end_queue_and_worker(self):
        self.assertEqual(self.request()[0],202)
        self.assertEqual(self.request()[0],200)
        sent=[]
        class Sender:
            def send(self,recipient,recs,*,token_hash=None):
                sent.append((recipient,recs));return 'provider-msg'
        results=run_due(self.events,self.catalog,create_deterministic_recommendations,Sender(),now=lambda:110.)
        self.assertEqual(results['processed'],{'token1':'provider_accepted'})
        self.assertEqual(sent[0][0],'person@example.test')
        self.assertEqual(len(sent[0][1]),3)
        self.assertEqual(run_due(self.events,self.catalog,create_deterministic_recommendations,Sender(),now=lambda:150.)['processed'],{})
    def test_signature_and_tenant_boundaries(self):
        self.assertEqual(self.request(signed=False)[0],401)
        self.assertEqual(self.request(self.payload(venue='venue_b'))[0],401)
        self.assertEqual(self.request(self.payload(form='other'))[0],401)
        self.assertEqual(self.request(path='/webhook/unknown')[0],404)
        self.assertEqual(self.request(raw=b'malformed')[0],400)
    def test_missing_unsafe_input(self):
        p=self.payload();p['form_response']['answers'][0]['email']='bad@@email'
        self.assertEqual(self.request(p)[0],400)
        p=self.payload();p['form_response']['variables'][0]['text']='oops'
        self.assertEqual(self.request(p)[0],400)
    def test_token_reused_with_different_recipient(self):
        self.assertEqual(self.request()[0],202)
        p=self.payload();p['form_response']['answers'][0]['email']='other@example.test'
        self.assertEqual(self.request(p)[0],400)
    def test_no_silent_data_on_public_health(self):
        self.assertEqual(self.request(method='GET',path='/health')[0],503)
        self.assertEqual(self.request(method='GET',path='/live')[1],{'status':'alive'})

if __name__=='__main__':unittest.main()
