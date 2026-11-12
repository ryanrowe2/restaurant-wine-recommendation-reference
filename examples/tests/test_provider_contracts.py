"""Fail-closed HTTP adapters and independent contract parity checks."""
import io
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from providers import PostmarkSender,StructuredRecommender,render_email,MODEL_OUTPUT_SCHEMA
from pipeline import Preferences,load_catalog,select_candidates,create_deterministic_recommendations,validate_recommendation_contract,ContractError
from delivery import PermanentProviderReject,SubmissionUncertain
import urllib.error

class FakeResponse:
    status=200
    def __init__(self,content,status=200):self.buffer=io.BytesIO(json.dumps(content).encode());self.status=status
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def read(self,n):return self.buffer.read(n)

class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.prefs=Preferences(category='red',sweetness='dry')
        self.selection=select_candidates(load_catalog(),'venue_a',self.prefs)
        self.recs=create_deterministic_recommendations(self.selection,self.prefs)
    def test_accepted_postmark_response_and_payload(self):
        captured=[]
        def open_(req,timeout):
            captured.append((req.get_full_url(),json.loads(req.data),req.get_header('X-postmark-server-token')))
            return FakeResponse({'ErrorCode':0,'MessageID':'message-123','To':'A@example.test'})
        sender=PostmarkSender('fictional-token','Sender@example.test',opener=open_)
        self.assertEqual(sender.send('A@EXAMPLE.TEST',self.recs),'message-123')
        self.assertEqual(captured[0][1]['To'],'A@example.test')
        self.assertIn('<h2>1.',captured[0][1]['HtmlBody'])
    def test_postmark_missing_message_id_is_uncertain(self):
        sender=PostmarkSender('fake','sender@example.test',opener=lambda r,timeout:FakeResponse({'ErrorCode':0}))
        with self.assertRaises(SubmissionUncertain):sender.send('a@example.test',self.recs)
    def test_postmark_500_is_uncertain_and_401_is_rejected(self):
        def failing(code):
            def func(req,timeout):raise urllib.error.HTTPError(req.full_url,code,'Test',{},None)
            return func
        for code,error in ((500,SubmissionUncertain),(401,PermanentProviderReject)):
            sender=PostmarkSender('fake','sender@example.test',opener=failing(code))
            with self.subTest(code=code),self.assertRaises(error):sender.send('a@example.test',self.recs)
    def test_openai_structured_response_validates_catalog(self):
        captured=[]
        def open_(req,timeout):
            captured.append(json.loads(req.data))
            return FakeResponse({'choices':[{'message':{'content':json.dumps({'recommendations':[{'wine_id':r['wine_id']} for r in self.recs]})}}]})
        generated=StructuredRecommender('fictional-key',opener=open_)(self.selection,self.prefs)
        self.assertEqual(generated,self.recs)
        self.assertEqual(captured[0]['response_format']['type'],'json_schema')
    def test_model_invented_wine_rejected(self):
        bad=[dict(item) for item in self.recs];bad[0]['wine_id']='invented'
        model=StructuredRecommender('fake',opener=lambda r,timeout:FakeResponse({'choices':[{'message':{'content':json.dumps({'recommendations':[{'wine_id':r['wine_id']} for r in bad]})}}]}))
        with self.assertRaises(ContractError):model(self.selection,self.prefs)
    def test_model_refusal_rejected(self):
        model=StructuredRecommender('fake',opener=lambda r,timeout:FakeResponse({'choices':[{'message':{'refusal':'not allowed','content':''}}]}))
        with self.assertRaises(ContractError):model(self.selection,self.prefs)
    def test_output_contract_schema_parity(self):
        path=Path(__file__).resolve().parents[1]/'recommendation_contract.json'
        schema=json.loads(path.read_text())
        self.assertEqual(schema['minItems'],3)
        self.assertEqual(schema['maxItems'],3)
        for f in ('wine_id','wine_name','pronunciation','origin','type','description'):
            self.assertEqual(schema['items']['properties'][f]['maxLength'],1500)
        try:import jsonschema
        except ImportError:return
        jsonschema.validate(self.recs,schema)
        oversized=[dict(x) for x in self.recs];oversized[0]['description']='x'*1501
        with self.assertRaises(ContractError):validate_recommendation_contract(oversized,self.selection.wines)
        with self.assertRaises(jsonschema.ValidationError):jsonschema.validate(oversized,schema)
        control=[dict(x) for x in self.recs];control[0]['description']='bad\x01data'
        with self.assertRaises(ContractError):validate_recommendation_contract(control,self.selection.wines)
        with self.assertRaises(jsonschema.ValidationError):jsonschema.validate(control,schema)

if __name__=='__main__':unittest.main()
