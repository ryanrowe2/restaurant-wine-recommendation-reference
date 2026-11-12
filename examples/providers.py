"""Isolated production service adapters. HTTP is mocked in offline tests."""
from __future__ import annotations
import html
from copy import deepcopy
import json
import math
import datetime
from email.utils import parsedate_to_datetime, getaddresses
import urllib.error
import urllib.request
from urllib.parse import urlsplit,quote
from typing import Callable
from pipeline import ContractError,InputError,Selection,Preferences,REQUIRED_FIELDS,validate_recommendation_contract,validate_selection_priority
from delivery import KnownNotAccepted,PermanentProviderReject,SubmissionUncertain,normalize_recipient

class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):
        return None

# Use the broadly supported strict JSON Schema subset; the exact count and
# nonempty, bounded, catalog-scoped ID rules are also checked locally.
MODEL_OUTPUT_SCHEMA={
 'type':'object','additionalProperties':False,'required':['recommendations'],
 'properties':{'recommendations':{'type':'array','minItems':3,'maxItems':3,'items':{
   'type':'object','additionalProperties':False,'required':['wine_id'],
   'properties':{'wine_id':{'type':'string'}}}}}
}

def render_email(recs:list[dict[str,str]])->str:
    """Minimal accessible HTML with all dynamic content escaped."""
    if len(recs)==0:
        return ('<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Wine recommendation update</title></head>'
                '<body><main><h1>Wine recommendation update</h1><p>We could not find three eligible wines at this venue for your selection. No recommendations have been invented.</p>'
                '<p>Please ask the restaurant for an updated wine list or speak to the staff.</p></main></body></html>')
    if len(recs)!=3 or any(not isinstance(rec,dict) or set(rec)!=set(REQUIRED_FIELDS) for rec in recs):
        raise ContractError('Only verified three-item recommendations may be rendered')
    body=[]
    for i,rec in enumerate(recs,1):
        for k in REQUIRED_FIELDS:
            if not isinstance(rec[k],str):raise ContractError('Invalid email field')
        h=lambda name:html.escape(rec[name],quote=True)
        display_type=html.escape(' '.join(word.capitalize() for word in rec['type'].replace('_',' ').split()),quote=True)
        body.append(f'<section><h2>{i}. {h("wine_name")}</h2><p>{h("origin")} · {display_type}</p><p>Pronunciation: {h("pronunciation")}</p><p>{h("description")}</p></section>')
    return ('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            '<title>Wine suggestions</title></head><body><main><h1>Your wine suggestions</h1>'+''.join(body)+'</main></body></html>')

def _post_json(url:str,headers:dict[str,str],payload:dict,timeout:float=15.,*,opener=None)->dict:
    host=urlsplit(url)
    if host.scheme!='https' or host.hostname not in ('api.openai.com','api.postmarkapp.com') or host.username or host.password:
        raise ValueError('Unapproved provider destination')
    if type(timeout) not in (int,float) or not 0<timeout<=60:raise ValueError('Invalid HTTP timeout')
    raw=json.dumps(payload).encode()
    request=urllib.request.Request(url,data=raw,headers={'Content-Type':'application/json',**headers},method='POST')
    # Never delegate provider redirects to the standard opener: it copies
    # Authorization and X-Postmark-Server-Token onto redirected requests.
    open_fn=opener or urllib.request.build_opener(_RejectRedirects()).open
    try:
        with open_fn(request,timeout=timeout) as response:
            if response.status not in (200,201,202):raise SubmissionUncertain('Unexpected provider status')
            bytes_out=response.read(1_000_001)
    except urllib.error.HTTPError as exc:
        # A documented 4xx rejection is a known non-acceptance. 5xx is uncertain.
        if exc.code==429:
            # An explicit provider rate limit is a known nonacceptance; honor
            # a bounded Retry-After instead of quarantining an unsent message.
            after=exc.headers.get('Retry-After') if exc.headers else None
            retry_after=None
            if after is not None:
                try:
                    retry_after=float(after)
                except (ValueError,TypeError,OverflowError):
                    try:
                        parsed=parsedate_to_datetime(after)
                        if parsed.tzinfo is None:parsed=parsed.replace(tzinfo=datetime.timezone.utc)
                        retry_after=(parsed-datetime.datetime.now(datetime.timezone.utc)).total_seconds()
                    except (ValueError,TypeError,OverflowError):pass
            if retry_after is not None and (not math.isfinite(retry_after) or retry_after<0):retry_after=None
            raise KnownNotAccepted('Provider rate limited request',retry_after=retry_after) from exc
        if exc.code in (400,401,403,404,422):raise PermanentProviderReject('Provider rejected request') from exc
        raise SubmissionUncertain('Provider response ambiguous') from exc
    except (OSError,TimeoutError,urllib.error.URLError) as exc:raise SubmissionUncertain('Provider acceptance not established') from exc
    if len(bytes_out)>1_000_000:raise SubmissionUncertain('Provider response too large')
    try:return json.loads(bytes_out)
    except (ValueError,UnicodeError,RecursionError) as exc:raise SubmissionUncertain('Unparseable provider response') from exc

class StructuredRecommender:
    """Actual OpenAI API adapter: must still validate membership after model output."""
    def __init__(self,api_key:str,model:str='gpt-4o-mini',*,opener=None):
        if not api_key:raise ValueError('OpenAI key required')
        self.api_key=api_key;self.model=model;self.opener=opener
    def __call__(self,selection:Selection,preferences:Preferences)->list[dict[str,str]]:
        catalog=[{'wine_id':w.wine_id,'wine_name':w.wine_name,'pronunciation':w.pronunciation,'origin':w.origin,'type':w.type,'sweetness':w.sweetness,'characteristics':sorted(w.characteristics),'pairings':sorted(w.pairings)} for w in selection.wines]
        schema=deepcopy(MODEL_OUTPUT_SCHEMA)
        schema['properties']['recommendations']['items']['properties']['wine_id']['enum']=[w.wine_id for w in selection.wines]
        payload={'model':self.model,'messages':[
          {'role':'system','content':'Rank exactly three distinct catalog wine_id values. Return only the ordered IDs. Never invent a wine.'},
          {'role':'user','content':json.dumps({'preferences':{'pairing':preferences.pairing,'category':preferences.category,'sweetness':preferences.sweetness,'characteristics':preferences.characteristics},'eligible_catalog':catalog})}],
          'response_format':{'type':'json_schema','json_schema':{'name':'restaurant_wine_recommendations','strict':True,'schema':schema}}}
        try:
            answer=_post_json('https://api.openai.com/v1/chat/completions',{'Authorization':'Bearer '+self.api_key},payload,opener=self.opener)
            if not isinstance(answer,dict) or not isinstance(answer.get('choices'),list) or not answer['choices']:
                raise ContractError('Model response envelope invalid')
            first=answer['choices'][0]
            if not isinstance(first,dict) or not isinstance(first.get('message'),dict):
                raise ContractError('Model message envelope invalid')
            choice=first['message']
            if choice.get('refusal'):raise ContractError('Model refused request')
            if not isinstance(choice.get('content'),str):raise ContractError('Model content invalid')
            parsed=json.loads(choice['content'])
            if not isinstance(parsed,dict):raise ContractError('Model payload invalid')
            output=parsed['recommendations']
        except (KeyError,TypeError,IndexError,ValueError,RecursionError,AttributeError) as exc:raise ContractError('Model response invalid') from exc
        if not isinstance(output,list) or len(output)!=3 or any(not isinstance(r,dict) or set(r)!={'wine_id'} or not isinstance(r['wine_id'],str) for r in output):
            raise ContractError('Model must return exactly three catalog IDs')
        if len({r['wine_id'] for r in output})!=3 or not {r['wine_id'] for r in output}.issubset({w.wine_id for w in selection.wines}):
            raise ContractError('Model returned duplicate or invented wine IDs')
        # Model may choose/order catalog IDs, but prose must not invent prices,
        # health benefits or stock claims. Rebuild every explanation from facts.
        from pipeline import create_deterministic_recommendations
        chosen={w.wine_id:w for w in selection.wines}
        ordered=Selection(tuple(chosen[rec['wine_id']] for rec in output),selection.stages_visited)
        grounded=create_deterministic_recommendations(ordered,preferences)
        validate_recommendation_contract(grounded,selection.wines,selection.wines[0].restaurant_id)
        validate_selection_priority(grounded,selection)
        return grounded

class PostmarkSender:
    """Postmark HTTP adapter; accepted means provider accepted, NOT delivered."""
    def __init__(self,server_token:str,from_email:str,*,opener=None):
        if not server_token:raise ValueError('Postmark server token required')
        self.server_token=server_token
        self.from_email=normalize_recipient(from_email,require_email=True)
        self.opener=opener
    def send(self,recipient:str,recs:list[dict[str,str]],*,token_hash:str|None=None)->str:
        to=normalize_recipient(recipient,require_email=True)
        payload={'From':self.from_email,'To':to,'Subject':('Your Personalized Wine Recommendations' if recs else 'Wine recommendation availability update'),'HtmlBody':render_email(recs),'MessageStream':'outbound'}
        if token_hash is not None:
            if not isinstance(token_hash,str) or len(token_hash)!=64 or any(c not in '0123456789abcdef' for c in token_hash):
                raise ValueError('Invalid event correlation hash')
            payload['Metadata']={'PalateEvent':token_hash}
        result=_post_json('https://api.postmarkapp.com/email',{'X-Postmark-Server-Token':self.server_token},payload,opener=self.opener)
        if not isinstance(result,dict):raise SubmissionUncertain('Unconfirmed Postmark response shape')
        if type(result.get('ErrorCode')) is int and result['ErrorCode']!=0:
            # A structured explicit error is affirmative rejection, not a timeout.
            raise PermanentProviderReject('Postmark rejected the submission')
        if (type(result.get('ErrorCode')) is not int or result['ErrorCode']!=0
            or not isinstance(result.get('MessageID'),str) or not result['MessageID'].strip()):
            raise SubmissionUncertain('Unconfirmed Postmark acceptance')
        # An HTTP 200 with a MessageID is insufficient if the provider reports
        # a different destination. Never mark another diner's send as ours.
        returned_to=result.get('To')
        if result.get('Cc') not in (None,'',[]) or result.get('Bcc') not in (None,'',[]):
            raise SubmissionUncertain('Postmark response includes unauthorized recipients')
        if not isinstance(returned_to,str):
            raise SubmissionUncertain('Postmark response missing destination')
        try:
            if normalize_recipient(returned_to,require_email=True)!=to:
                raise SubmissionUncertain('Postmark response destination mismatch')
        except (InputError,ValueError,TypeError) as error:
            raise SubmissionUncertain('Postmark response destination invalid') from error
        return result['MessageID']


class PostmarkReceiptVerifier:
    """Read-only verification of a *specific* Postmark message, recipient and event.

    A receipt can confirm provider acceptance, not mailbox delivery. If Postmark
    has expired the message or network verification fails, the event stays held.
    """
    def __init__(self,server_token:str,from_email:str,*,opener=None):
        if not isinstance(server_token,str) or not server_token.strip():
            raise ValueError('Postmark server token required')
        self.server_token=server_token
        self.from_email=normalize_recipient(from_email,require_email=True)
        self.opener=opener
    def __call__(self,provider_ref:str,recipient:str,token_hash:str)->bool:
        if (not isinstance(provider_ref,str) or not 1<=len(provider_ref)<=255
            or not all(ch.isascii() and (ch.isalnum() or ch in '-_') for ch in provider_ref)):
            return False
        if not isinstance(token_hash,str) or len(token_hash)!=64 or any(c not in '0123456789abcdef' for c in token_hash):
            return False
        expected=normalize_recipient(recipient,require_email=True)
        request=urllib.request.Request(
            'https://api.postmarkapp.com/messages/outbound/'+quote(provider_ref,safe='')+'/details',
            headers={'Accept':'application/json','X-Postmark-Server-Token':self.server_token},method='GET')
        opener=self.opener or urllib.request.build_opener(_RejectRedirects()).open
        try:
            with opener(request,timeout=15.) as response:
                if response.status!=200:return False
                raw=response.read(1_000_001)
                if len(raw)>1_000_000:return False
                data=json.loads(raw)
        except (OSError,TimeoutError,urllib.error.URLError,ValueError,UnicodeError,RecursionError):
            return False
        if not isinstance(data,dict) or data.get('MessageID')!=provider_ref:
            return False
        if data.get('Status') not in ('Sent','Processed','Queued'):
            return False
        try:
            sender_field=data.get('From')
            if not isinstance(sender_field,str):return False
            addresses=getaddresses([sender_field])
            if len(addresses)!=1 or not addresses[0][1]:return False
            if normalize_recipient(addresses[0][1],require_email=True)!=self.from_email:
                return False
        except (ValueError,TypeError):
            return False
        metadata=data.get('Metadata')
        if not isinstance(metadata,dict) or metadata.get('PalateEvent')!=token_hash:
            return False
        # Confirm the *entire* send was to the one authorized diner.
        # Postmark may expose To as a comma-separated string and Recipients as
        # an expanded list. Cc/Bcc must both be absent or empty, and if both
        # To and Recipients are populated they must agree exactly.
        for field in ('Cc','Bcc'):
            value=data.get(field)
            if value not in (None,'',[]):
                return False
        def recipient_list(raw):
            if isinstance(raw,str):
                addresses=[address.strip() for address in raw.split(',')]
            elif isinstance(raw,list):
                addresses=[r.get('Email') if isinstance(r,dict) else r for r in raw]
            else:
                return None
            if len(addresses)!=1:
                return None
            try:
                return [normalize_recipient(addresses[0],require_email=True)]
            except (TypeError,ValueError):
                return None
        received=data.get('Recipients')
        destination=data.get('To')
        if received is None and destination is None:return False
        if received is not None and recipient_list(received)!=[expected]:return False
        if destination is not None and recipient_list(destination)!=[expected]:return False
        return True
