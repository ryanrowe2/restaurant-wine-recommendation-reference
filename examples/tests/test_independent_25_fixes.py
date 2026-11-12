"""Regression coverage for independently confirmed November 11 security/logic findings."""
from __future__ import annotations
from claim_fixture import claim_nonce
import base64
import hashlib
import hmac
import io
import json
from datetime import datetime,timezone
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

HERE=Path(__file__).resolve().parents[1]
ROOT=HERE.parent
sys.path.insert(0,str(HERE))
sys.path.insert(0,str(ROOT/'scripts'))
from pipeline import Wine, Preferences, InputError, create_deterministic_recommendations,load_catalog,select_candidates
from delivery import EventStore, fingerprint_for,process_event,normalize_recipient
from providers import _RejectRedirects, MODEL_OUTPUT_SCHEMA,render_email, StructuredRecommender
from webhook import FormBinding, verify_and_parse
from runtime import run_due,config_bindings
from webapp import WebhookApp
from catalog_store import CatalogStore
from postgres_catalog import legacy_row_to_wine,extract_reviewable_pairings
import release_gate

class HardenedTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=EventStore(Path(self.tmp.name)/'events.sqlite3');self.addCleanup(self.store.close)
        self.catalog=CatalogStore(Path(self.tmp.name)/'wines.sqlite3');self.addCleanup(self.catalog.close)
        self.catalog.replace_venue('venue_a',[x for x in load_catalog() if x.restaurant_id=='venue_a'])
    def test_provider_redirect_is_never_followed(self):
        import urllib.request
        req=urllib.request.Request('https://api.openai.com/v1/chat/completions',headers={'Authorization':'Bearer FAKE'},data=b'{}')
        self.assertIsNone(_RejectRedirects().redirect_request(req,None,302,'Moved',{'Location':'https://evil.invalid'},'https://evil.invalid'))
    def signed(self,event_type='form_response'):
        binding=FormBinding('f_1','venue_a',b'fictional-secret-12345')
        body=json.dumps({'event_type':event_type,'form_response':{'form_id':'f_1','token':'tx_1','submitted_at':datetime.now(timezone.utc).isoformat(),'hidden':{'restaurant_id':'venue_a'},'answers':[{'type':'email','email':'alice@example.test'}],
             'variables':[{'key':'wine_category','type':'text','text':'red'},{'key':'sweetness','type':'text','text':'dry'}]}}).encode()
        sig='sha256='+base64.b64encode(hmac.new(binding.secret,body,hashlib.sha256).digest()).decode()
        return binding,body,sig
    def test_partial_webhook_is_rejected_after_valid_signature(self):
        binding,body,sig=self.signed('form_response_partial')
        with self.assertRaises(InputError):verify_and_parse(body,sig,binding)
        binding,body,sig=self.signed()
        self.assertEqual(verify_and_parse(body,sig,binding).venue_id,'venue_a')
    def test_duplicate_form_authorization_rejected(self):
        secret=b'fictional-secret-12345'
        with self.assertRaises(ValueError):WebhookApp(self.store,{'a':FormBinding('f','venue_a',secret),'b':FormBinding('f','venue_b',secret)})
        env={'TYPEFORM_BINDINGS_JSON':json.dumps({'a':{'form_id':'f','venue_id':'venue_a','secret_env':'TYPEFORM_SECRET_A'},
             'b':{'form_id':'f','venue_id':'venue_b','secret_env':'TYPEFORM_SECRET_A'}}),'TYPEFORM_SECRET_A':'fictional-secret-12345'}
        with self.assertRaises(RuntimeError):config_bindings(env)
    def test_corrupt_event_is_quarantined_and_valid_jobs_continue(self):
        p=Preferences(category='red',sweetness='dry')
        self.store.enqueue_request(token='bad',venue='venue_a',recipient='bad@example.test',preferences=p,now=10)
        self.store.enqueue_request(token='good',venue='venue_a',recipient='good@example.test',preferences=p,now=11)
        with self.store._connect() as c:c.execute("UPDATE events SET request_json=? WHERE token='bad'",('not json',))
        result=run_due(self.store,self.catalog,create_deterministic_recommendations,
             type('Sender',(),{'send':lambda self,recipient,recs,token_hash=None:'sent'})(),now=lambda:20.)
        self.assertEqual(result['processed'].get('good'),'provider_accepted')
        self.assertEqual(self.store.summary('bad')[0],'permanent_failure')
    def test_semantically_invalid_event_is_quarantined(self):
        p=Preferences(category='red',sweetness='dry')
        self.store.enqueue_request(token='bad2',venue='venue_a',recipient='bad@example.test',preferences=p,now=10)
        with self.store._connect() as c:c.execute("UPDATE events SET request_json=? WHERE token='bad2'",('{}',))
        out=run_due(self.store,self.catalog,create_deterministic_recommendations,
            type('Sender',(),{'send':lambda self,recipient,recs,token_hash=None:'sent'})(),now=lambda:20.)
        self.assertNotIn('bad2',out['processed']) # rejected before scheduling
        self.assertEqual(self.store.summary('bad2')[0],'permanent_failure')
        self.assertFalse(self.store.due_requests(now=21))
    def test_empty_string_venue_in_stored_json_is_quarantined(self):
        self.store.enqueue_request(token='badvenue',venue='venue_a',recipient='bad@example.test',preferences=Preferences(),now=10)
        with self.store._connect() as c:
            c.execute('UPDATE events SET request_json=? WHERE token=?',(json.dumps({'venue':'','recipient':'bad@example.test','preferences':{}}),'badvenue'))
        result=run_due(self.store,self.catalog,create_deterministic_recommendations,
            type('Sender',(),{'send':lambda self,recipient,recs,token_hash=None:'sent'})(),now=lambda:20.)
        self.assertNotIn('badvenue',result['processed']) # rejected before scheduling
        self.assertEqual(self.store.summary('badvenue')[0],'permanent_failure')
    def test_sqlite_ingress_failure_is_safe_503(self):
        binding,body,sig=self.signed()
        app=WebhookApp(self.store,{'a':binding})
        env={'REQUEST_METHOD':'POST','PATH_INFO':'/webhook/a','CONTENT_TYPE':'application/json',
             'CONTENT_LENGTH':str(len(body)),'wsgi.input':io.BytesIO(body),'HTTP_TYPEFORM_SIGNATURE':sig}
        with patch.object(self.store,'enqueue_request',side_effect=sqlite3.OperationalError('disk full')):
            status=[];response=app(env,lambda s,h:status.append(s))
        self.assertEqual(status[0][:3],'503')
        self.assertEqual(json.loads(b''.join(response)),{'error':'temporarily_unavailable'})
    def test_lease_extension_must_be_positive_and_bounded(self):
        p=Preferences()
        fp=fingerprint_for('venue_a','a@example.test',p)
        self.store.enqueue_request(token='t',venue='venue_a',recipient='a@example.test',preferences=p,now=99)
        self.assertEqual(self.store.claim('t',fp,now=100).status,'claimed')
        for seconds in (0,-1,float('nan'),float('inf'),301):
            with self.subTest(seconds=seconds),self.assertRaises((InputError,ValueError)):
                self.store.heartbeat('t',1,now=101,lease_seconds=seconds,nonce=claim_nonce(self.store,'t',1))
        self.assertEqual(self.store.claim('t',fp,now=102).status,'in_flight')
    def test_sweet_exact_precedes_semi_dry(self):
        base=load_catalog()[0]
        wines=[]
        for i in range(6):
            wines.append(replace(base,restaurant_id='v',wine_id=f'w{i}',sweetness='semi-dry' if i<3 else 'sweet',category='red'))
        result=select_candidates(wines,'v',Preferences(sweetness='sweet'))
        self.assertEqual([w.sweetness for w in result.wines],['sweet']*3)
    def test_model_can_evaluate_bounded_pool(self):
        result=select_candidates(load_catalog(),'venue_a',Preferences(),max_candidates=12)
        self.assertEqual(len(result.wines),6)
        self.assertEqual(len(create_deterministic_recommendations(result,Preferences())),3)
    def test_blank_external_schema_and_python_reject(self):
        try:from jsonschema import validate,ValidationError
        except ImportError:self.skipTest('Optional JSON schema verifier not installed')
        schema=json.loads((HERE/'recommendation_contract.json').read_text())
        selection=select_candidates(load_catalog(),'venue_a',Preferences())
        recs=create_deterministic_recommendations(selection,Preferences())
        recs[0]['description']='    '
        with self.assertRaises(ValidationError):validate(recs,schema)
    def test_structured_schema_enforces_three_and_local_validation(self):
        arr=MODEL_OUTPUT_SCHEMA['properties']['recommendations']
        self.assertEqual((arr['minItems'],arr['maxItems']),(3,3))
        self.assertEqual(arr['items']['properties']['wine_id'],{'type':'string'})  # unsupported API length keywords omitted
        self.assertEqual(arr['items']['required'],['wine_id'])
    def test_malformed_dot_atom_email_is_rejected(self):
        for a in ('.a@example.com','a.@example.com','a..b@example.com'):
            with self.subTest(a=a),self.assertRaises(InputError):normalize_recipient(a,require_email=True)
    def test_unresolved_payload_is_scrubbed_after_policy_age(self):
        p=Preferences()
        self.store.enqueue_request(token='old',venue='venue_a',recipient='old@example.test',preferences=p,now=10)
        fp=fingerprint_for('venue_a','old@example.test',p)
        self.assertEqual(self.store.claim('old',fp,now=10).status,'claimed')
        self.store.mark_dispatch_started('old',1,now=11,nonce=claim_nonce(self.store,'old',1))
        self.store.mark_uncertain('old',1,now=12,nonce=claim_nonce(self.store,'old',1))
        self.assertEqual(self.store.scrub_aged_unresolved(now=100*86400),1)
        with self.store._connect() as c:
            self.assertIsNone(c.execute('SELECT request_json FROM events WHERE token=?',('old',)).fetchone()[0])
    def test_health_reflects_worker_state(self):
        self.assertFalse(self.store.readiness(now=100)['ready'])
        self.store.worker_heartbeat(now=100)
        self.assertTrue(self.store.readiness(now=110)['ready'])
        self.assertFalse(self.store.readiness(now=300)['ready'])
    def test_operational_alert_threshold(self):
        self.store.enqueue_request(token='late',venue='venue_a',recipient='a@example.test',preferences=Preferences(),now=0)
        self.assertIn('stale_queue',[x['code'] for x in self.store.alerts(now=1000)])
    def test_no_match_notice_is_truthful_and_sent_once(self):
        out=[]
        result=process_event(self.store,token='sparse',claimed_venue='venue_sparse',authenticated_venue='venue_sparse',
              recipient_key='a@example.test',preferences=Preferences(),sender=lambda recs:(out.append(recs) or 'provider_accept'),
              now=100,notify_insufficient=True)
        self.assertEqual(result,'provider_accepted')
        self.assertEqual(out,[[]])
        self.assertIn('could not find three eligible wines',render_email([]).lower())
    def test_long_pre_send_work_renews_lease(self):
        import delivery
        base=time.monotonic()
        simulated_clock=lambda:100+(time.monotonic()-base)*100
        def slow_recommender(selection,prefs):
            time.sleep(0.42)  # Greater than the simulated 30-second claim lease.
            return create_deterministic_recommendations(selection,prefs)
        with patch.object(delivery,'HEARTBEAT_INTERVAL_SECONDS',0.02):
            status=process_event(self.store,token='heartbeat_job',claimed_venue='venue_a',
                authenticated_venue='venue_a',recipient_key='a@example.test',preferences=Preferences(),
                sender=lambda recs:'accepted',now=100,catalog_store=self.catalog,
                recommender=slow_recommender,clock=simulated_clock)
        self.assertEqual(status,'provider_accepted')
    def test_model_explanations_are_replaced_with_catalog_facts(self):
        import providers
        selection=select_candidates(load_catalog(),'venue_a',Preferences(),max_candidates=12)
        base=create_deterministic_recommendations(selection,Preferences())
        base[0]['description']='Guaranteed miracle cure and $5 price.'
        raw=json.dumps({'choices':[{'message':{'content':json.dumps({'recommendations':[{'wine_id':r['wine_id']} for r in base]})}}]}).encode()
        class Response:
            status=200
            def __enter__(self):return self
            def __exit__(self,*args):return False
            def read(self,n):return raw
        result=providers.StructuredRecommender('FAKE_KEY',opener=lambda request,timeout:Response())(selection,Preferences())
        self.assertNotIn('miracle',result[0]['description'])
        self.assertNotIn('$5',result[0]['description'])
        self.assertEqual(result[0]['wine_id'],base[0]['wine_id'])
    def test_legacy_pairing_has_traceable_tags(self):
        row=(1,'Example Wine',True,False,'Origin','red','merlot','notes','full','dry','Pair with steak and roast lamb',None)
        wine=legacy_row_to_wine(row)
        self.assertEqual(wine.pairings,frozenset())  # Unreviewed inference never ranks
        self.assertIn('red_meat',extract_reviewable_pairings(row[10]))
        self.assertNotIn('fish',wine.pairings)
        self.assertFalse(extract_reviewable_pairings('unknown abstract dish'))
    def test_no_credential_on_directory_symlink_or_plain_password(self):
        with tempfile.TemporaryDirectory() as d:
            base=Path(d);target=Path(d)/'protected';target.mkdir();(target/'private.txt').write_text('password='+'HiddenPass1234567')
            (base/'link').symlink_to(target,target_is_directory=True)
            problems=release_gate.scan(base)
            self.assertTrue(any('link: symbolic link' in x for x in problems))
            self.assertTrue(any('possible' in x or 'suspicious' in x for x in problems))
    def test_redacted_audit_survives_terminal_purge(self):
        p=Preferences()
        self.store.enqueue_request(token='audit',venue='venue_a',recipient='a@example.test',preferences=p,now=0)
        fp=fingerprint_for('venue_a','a@example.test',p)
        self.store.claim('audit',fp,now=0)
        self.store.mark_permanent_failure('audit',1,now=1,nonce=claim_nonce(self.store,'audit',1))
        self.store.purge_terminal(now=40*86400)
        self.assertIsNone(self.store.summary('audit'))
        with self.store._connect() as c:self.assertGreater(c.execute('SELECT count(*) FROM event_audit').fetchone()[0],0)
    def test_migration_version_and_constraint_present(self):
        pg=(HERE/'postgres_catalog.py').read_text()
        sql=(HERE/'migrations/002_validate_tags.sql').read_text()
        self.assertIn('schema_migrations',pg)
        self.assertIn('Schema drift detected',pg)
        self.assertIn('valid_tag_array',sql)
        self.assertIn('ADD CONSTRAINT wine_pairings_valid',sql)

if __name__=='__main__':unittest.main()
