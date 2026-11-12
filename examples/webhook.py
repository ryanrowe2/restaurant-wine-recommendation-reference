"""Authentication and strict Typeform payload translation for the WSGI endpoint."""
from __future__ import annotations
import json
import math
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from pipeline import Preferences,InputError,CATEGORIES,SWEETNESS,valid_venue_id
from security import verify_typeform_signature,MAX_BODY_BYTES
from delivery import normalize_recipient,EventStore

MAX_VARIABLES=100
# Typeform retries normally complete within hours. Allow a full day and 5m skew.
MAX_SUBMISSION_AGE_SECONDS=24*3600
MAX_FUTURE_SKEW_SECONDS=300
KNOWN_TAGS=frozenset(('fruity','bold','versatile','refreshing','floral','rich','fresh','deep','light','spice','berries','tobacco','pairs_with_red_meat','pairs_with_fish','pairs_with_spicy_food','pairs_with_dessert'))

@dataclass(frozen=True)
class FormBinding:
    form_id:str
    venue_id:str
    secret:bytes
    hidden_restaurant_id:str|None=None
    def __post_init__(self):
        if (not isinstance(self.form_id,str) or not 0<len(self.form_id)<=128 or self.form_id.strip()!=self.form_id or
            not valid_venue_id(self.venue_id) or self.venue_id.strip()!=self.venue_id or
            not isinstance(self.secret,bytes) or len(self.secret)<16):
            raise ValueError('Invalid trusted form binding')
        if self.hidden_restaurant_id is not None and (not valid_venue_id(self.hidden_restaurant_id) or self.hidden_restaurant_id.strip()!=self.hidden_restaurant_id):
            raise ValueError('Invalid hidden restaurant ID binding')

@dataclass(frozen=True)
class VerifiedRequest:
    venue_id:str
    token:str
    recipient:str
    preferences:Preferences

def verify_and_parse(raw_body:bytes,signature:str,binding:FormBinding,*,now:float|None=None)->VerifiedRequest:
    if not verify_typeform_signature(raw_body,signature,binding.secret):
        raise PermissionError('Unauthenticated webhook')
    try:
        data=json.loads(raw_body)
    except (ValueError,UnicodeError,RecursionError) as exc:raise InputError('Malformed JSON') from exc
    if not isinstance(data,dict) or not isinstance(data.get('form_response'),dict):raise InputError('Missing form response')
    if data.get('event_type')!='form_response':raise InputError('Only completed form responses are accepted')
    response=data['form_response']
    # HMAC signs the submission time as part of the raw body. Signature alone
    # cannot prevent captured deliveries from being replayed after retention.
    observed=time.time() if now is None else now
    if type(observed) not in (int,float) or not math.isfinite(observed) or observed<0:
        raise InputError('Invalid server clock')
    submitted=response.get('submitted_at')
    if not isinstance(submitted,str) or len(submitted)>40:
        raise InputError('Signed submission timestamp required')
    try:
        parsed=datetime.fromisoformat(submitted.replace('Z','+00:00'))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError('Timestamp must contain a UTC offset')
        stamp=parsed.astimezone(timezone.utc).timestamp()
    except (ValueError,OverflowError,TypeError) as exc:
        raise InputError('Invalid signed submission timestamp') from exc
    if not math.isfinite(stamp) or observed-stamp>MAX_SUBMISSION_AGE_SECONDS or stamp-observed>MAX_FUTURE_SKEW_SECONDS:
        raise InputError('Stale or future signed submission')
    if response.get('form_id')!=binding.form_id:raise PermissionError('Unexpected Typeform form ID')
    hidden=response.get('hidden')
    if not isinstance(hidden,dict) or hidden.get('restaurant_id')!=(binding.hidden_restaurant_id or binding.venue_id):
        raise PermissionError('Venue binding mismatch')
    token=response.get('token')
    EventStore._check_token(token)
    answers=response.get('answers')
    if not isinstance(answers,list) or len(answers)>100:raise InputError('Invalid answers')
    emails=[answer.get('email') for answer in answers if isinstance(answer,dict) and answer.get('type')=='email']
    if len(emails)!=1:raise InputError('Exactly one email is required')
    recipient=normalize_recipient(emails[0],require_email=True)
    variables=response.get('variables')
    if not isinstance(variables,list) or len(variables)>MAX_VARIABLES:raise InputError('Invalid variables')
    values={};characteristics=[];seen_keys=set()
    for var in variables:
        if not isinstance(var,dict) or not isinstance(var.get('key'),str):raise InputError('Invalid variable item')
        key=var['key']
        if key in seen_keys:raise InputError('Duplicate variable key')
        seen_keys.add(key)
        kind=var.get('type')
        if key in KNOWN_TAGS:
            if kind!='number' or type(var.get('number')) not in (int,float) or var['number'] not in (0,1):raise InputError('Invalid feature flag')
            if var['number']==1:characteristics.append(key)
        elif key in ('wine_category','sweetness','pairing','by_glass','by_bottle'):
            if kind!='text' or not isinstance(var.get('text'),str) or len(var['text'])>64:raise InputError('Invalid preference')
            values[key]=var['text'].strip().lower()
        # Unknown Typeform variables are excluded from the safety-critical contract.
    if values.get('wine_category') not in CATEGORIES or values.get('sweetness','').replace('_','-') not in SWEETNESS:
        raise InputError('Missing/invalid category or sweetness')
    flags={}
    for field in ('by_glass','by_bottle'):
        if field in values:
            if values[field] not in ('true','false'):raise InputError('Invalid serving flag')
            flags[field]=values[field]=='true'
    if flags and not any(flags.values()):raise InputError('No serving format selected')
    if flags.get('by_glass') and not flags.get('by_bottle'):serving='glass'
    elif flags.get('by_bottle') and not flags.get('by_glass'):serving='bottle'
    else:serving='either'
    prefs=Preferences(category=values['wine_category'],sweetness=values['sweetness'],characteristics=tuple(characteristics),serving_format=serving,pairing=values.get('pairing',''))
    return VerifiedRequest(binding.venue_id,token,recipient,prefs)
