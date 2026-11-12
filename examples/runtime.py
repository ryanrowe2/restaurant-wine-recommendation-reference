"""Composition root for authenticated HTTP ingress and independent delivery worker."""
from __future__ import annotations
import json
import hashlib
import os
import time
from pathlib import Path
from catalog_store import CatalogStore
from delivery import EventStore,process_event,normalize_recipient
from pipeline import Preferences
from providers import PostmarkSender,StructuredRecommender
from webhook import FormBinding
from webapp import WebhookApp

def config_bindings(environ=None):
    env=environ if environ is not None else os.environ
    raw=env.get('TYPEFORM_BINDINGS_JSON','')
    try: data=json.loads(raw)
    except (ValueError,TypeError) as e:raise RuntimeError('Trusted TYPEFORM_BINDINGS_JSON required') from e
    if not isinstance(data,dict) or not data:raise RuntimeError('At least one form binding required')
    out={}
    registered_forms=set()
    registered_secret_envs=set()
    registered_secret_values=set()
    for alias,config in data.items():
        if not isinstance(alias,str) or not alias.replace('_','').replace('-','').isalnum() or not isinstance(config,dict):raise RuntimeError('Invalid form binding alias')
        keyname=config.get('secret_env')
        if not isinstance(keyname,str) or not keyname.startswith('TYPEFORM_SECRET_'):raise RuntimeError('Webhook secret environment variable required')
        secret=env.get(keyname,'').encode('utf-8')
        if keyname in registered_secret_envs or secret in registered_secret_values:
            raise RuntimeError('Each Typeform form must have its own unique signing key and environment variable')
        registered_secret_envs.add(keyname)
        registered_secret_values.add(secret)
        if 'form_id' not in config or 'venue_id' not in config:raise RuntimeError('Incomplete trusted form binding')
        binding=FormBinding(form_id=config['form_id'],venue_id=config['venue_id'],secret=secret,hidden_restaurant_id=config.get('hidden_restaurant_id'))
        if binding.form_id in registered_forms:raise RuntimeError('A Typeform form may authorize only one venue')
        registered_forms.add(binding.form_id)
        out[alias]=binding
    return out

def _configured_catalog(env):
    if env.get('PG_DSN'):
        from postgres_catalog import PostgresCatalogStore,connect_from_env
        return PostgresCatalogStore(connect_from_env(dsn=env['PG_DSN']))
    path=env.get('CATALOG_DB_PATH')
    if path and path!=':memory:':
        return CatalogStore(path)
    raise RuntimeError('Persistent catalog DB path or PG_DSN required')

def create_http_app(environ=None):
    env=environ if environ is not None else os.environ
    state_path=env.get('EVENT_DB_PATH')
    if not state_path or state_path==':memory:':raise RuntimeError('Persistent EVENT_DB_PATH required for HTTP service')
    trusted=config_bindings(env)
    catalog=_configured_catalog(env)
    return WebhookApp(EventStore(state_path),trusted,catalog=catalog)

def create_worker_components(environ=None):
    env=environ if environ is not None else os.environ
    needed=('EVENT_DB_PATH','OPENAI_API_KEY','POSTMARK_SERVER_TOKEN','SENDER_EMAIL')
    if any(not env.get(key) for key in needed):raise RuntimeError('Missing worker configuration')
    if env['EVENT_DB_PATH']==':memory:':raise RuntimeError('Worker requires durable storage')
    catalog=_configured_catalog(env)
    return (EventStore(env['EVENT_DB_PATH']),catalog,
            StructuredRecommender(env['OPENAI_API_KEY'],model=env.get('OPENAI_MODEL','gpt-4o-mini')),
            PostmarkSender(env['POSTMARK_SERVER_TOKEN'],env['SENDER_EMAIL']))

def run_due(store:EventStore,catalog:CatalogStore,recommender,sender,*,now=time.time,limit=50)->dict:
    """One safe scheduler cycle; caller runs it on a repeated interval."""
    store.worker_heartbeat(now=now())
    count_held=store.reap_expired_submissions(now=now())
    due=store.due_requests(now=now(),limit=limit)
    results={}
    for token,payload in due:
        try:
            if set(payload)!= {'venue','recipient','preferences'}:raise ValueError('Unexpected queued request fields')
            venue=payload['venue']
            recipient=payload['recipient']
            if not isinstance(venue,str) or not venue.strip() or len(venue)>128 or not isinstance(recipient,str) or not isinstance(payload.get('preferences'),dict):raise ValueError('Malformed queued request')
            recipient=normalize_recipient(recipient,require_email=True)
            preferences=Preferences(**payload['preferences'])
            results[token]=process_event(store,token=token,claimed_venue=venue,authenticated_venue=venue,
             recipient_key=recipient,preferences=preferences,catalog_store=catalog,recommender=recommender,
             sender=lambda recs,target=recipient,token=token:sender.send(target,recs,token_hash=hashlib.sha256(token.encode()).hexdigest()),now=now(),clock=now,notify_insufficient=True)
        except (KeyError,TypeError,ValueError,PermissionError) as exc:
            # These are poison messages, not transient provider failures.
            store.poison(token,now=now(),reason='invalid_stored_request')
            results[token]='invalid_stored_request'
        except Exception:
            # Potential provider acceptance: do not automatically resend.
            # The reaper will quarantine an uncertain submission.
            results[token]='worker_error_review_state'
    return {'processed':results,'uncertain_submissions_held':count_held}
