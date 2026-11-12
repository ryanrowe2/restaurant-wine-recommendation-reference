"""Adversarial concurrency, long-term duplicate and legacy identifier checks."""
import base64
import concurrent.futures
import hashlib
import hmac
import io
import json
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from delivery import EventStore,process_event,fingerprint_for,TokenCollision
from pipeline import Preferences,load_catalog
from catalog_store import CatalogStore
from webhook import FormBinding
from webapp import WebhookApp

class ConcurrencyAndRetention(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=EventStore(Path(self.tmp.name)/'events.db');self.addCleanup(self.store.close)
        self.catalog=CatalogStore(Path(self.tmp.name)/'catalog.db');self.addCleanup(self.catalog.close)
        self.catalog.replace_venue('venue_a',[w for w in load_catalog() if w.restaurant_id=='venue_a'])
        self.prefs=Preferences(category='red',sweetness='dry')
    def test_competing_workers_submit_only_once(self):
        hits=[];lock=threading.Lock()
        def send(recs):
            with lock:hits.append(1)
            time.sleep(0.025)
            return 'provider-ref'
        def attempt(_):
            return process_event(self.store,token='same-token',claimed_venue='venue_a',authenticated_venue='venue_a',
                                 recipient_key='a@example.test',preferences=self.prefs,now=100,
                                 sender=send,catalog_store=self.catalog)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results=list(pool.map(attempt,range(8)))
        self.assertEqual(len(hits),1)
        self.assertEqual(results.count('provider_accepted'),1)
        self.assertTrue(all(s in ('provider_accepted','in_flight','already_accepted') for s in results))
    def test_purged_tombstone_blocks_late_duplicate(self):
        self.assertEqual(process_event(self.store,token='same-token',claimed_venue='venue_a',authenticated_venue='venue_a',
          recipient_key='a@example.test',preferences=self.prefs,sender=lambda recs:'accepted',now=100,catalog_store=self.catalog),'provider_accepted')
        self.assertEqual(self.store.purge_terminal(now=31*86400),1)
        self.assertIsNone(self.store.summary('same-token'))
        result=process_event(self.store,token='same-token',claimed_venue='venue_a',authenticated_venue='venue_a',
          recipient_key='a@example.test',preferences=self.prefs,sender=lambda recs:(_ for _ in ()).throw(AssertionError('resend')),now=31*86400+3,catalog_store=self.catalog)
        self.assertEqual(result,'already_accepted')
        with self.assertRaises(TokenCollision):self.store.claim('same-token','different-fingerprint',now=31*86400+4)
    def test_unknown_venue_is_operational_retry_not_customer_rejection(self):
        result=process_event(self.store,token='missing-venue',claimed_venue='unknown',authenticated_venue='unknown',
          recipient_key='a@example.test',preferences=self.prefs,sender=lambda recs:'sent',now=100,catalog_store=self.catalog)
        self.assertEqual(result,'retryable_failure')
    def test_historical_form_venue_mapping(self):
        secret=b'made-up-typeform-secret-abc'
        app=WebhookApp(self.store,{'legacy':FormBinding('form-1','legacy-1',secret,hidden_restaurant_id='1')},clock=lambda:100.)
        body=json.dumps({'event_type':'form_response','form_response':{'form_id':'form-1','token':'token-1','submitted_at':'1970-01-01T00:01:40Z','hidden':{'restaurant_id':'1'},
          'answers':[{'type':'email','email':'a@example.test'}],
          'variables':[{'key':'wine_category','type':'text','text':'red'}, {'key':'sweetness','type':'text','text':'dry'}]}}).encode()
        signature='sha256='+base64.b64encode(hmac.new(secret,body,hashlib.sha256).digest()).decode()
        environ={'PATH_INFO':'/webhook/legacy','REQUEST_METHOD':'POST','CONTENT_TYPE':'application/json','CONTENT_LENGTH':str(len(body)),
          'HTTP_TYPEFORM_SIGNATURE':signature,'wsgi.input':io.BytesIO(body)}
        statuses=[]
        app(environ,lambda status,headers:statuses.append(status))
        self.assertEqual(statuses[0].split()[0],'202')
        self.assertEqual(self.store.due_requests(now=101)[0][1]['venue'],'legacy-1')
    def test_oversized_content_length_rejected_before_read(self):
        app=WebhookApp(self.store,{'a':FormBinding('form-1','venue_a',b'secret-1234567890')})
        statuses=[]
        app({'PATH_INFO':'/webhook/a','REQUEST_METHOD':'POST','CONTENT_TYPE':'application/json',
             'CONTENT_LENGTH':'9'*1000,'wsgi.input':io.BytesIO(b'')},lambda status,headers:statuses.append(status))
        self.assertEqual(statuses[0].split()[0],'413')

if __name__=='__main__':unittest.main()
