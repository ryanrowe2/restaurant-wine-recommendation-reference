"""Python-standard-library WSGI webhook app, compatible with Waitress/Gunicorn.

POST /webhook/<trusted-form-alias> verifies raw HMAC before parsing data. This
independently authored reference endpoint does not import historical Flask code.
"""
from __future__ import annotations
import json
import time
import sqlite3
from typing import Mapping
from pipeline import InputError
from delivery import EventStore,TokenCollision
from webhook import FormBinding,verify_and_parse
from security import MAX_BODY_BYTES

class WebhookApp:
    def __init__(self,store:EventStore,bindings:Mapping[str,FormBinding],*,clock=time.time,catalog=None):
        if not bindings or not all(isinstance(k,str) and isinstance(v,FormBinding) for k,v in bindings.items()):
            raise ValueError('Trusted bindings required')
        if len({b.form_id for b in bindings.values()})!=len(bindings):raise ValueError('Ambiguous duplicate form ownership')
        if len({b.secret for b in bindings.values()})!=len(bindings):raise ValueError('A signing key may authorize only one form')
        self.store=store;self.bindings=dict(bindings);self.clock=clock;self.catalog=catalog
    def __call__(self,environ,start_response):
        def reply(status:int,data:dict):
            status_text={200:'200 OK',202:'202 Accepted',400:'400 Bad Request',401:'401 Unauthorized',404:'404 Not Found',405:'405 Method Not Allowed',413:'413 Content Too Large',415:'415 Unsupported Media Type',429:'429 Too Many Requests',503:'503 Service Unavailable'}[status]
            body=json.dumps(data,separators=(',',':')).encode()
            start_response(status_text,[('Content-Type','application/json'),('Content-Length',str(len(body))),('Cache-Control','no-store')]);return [body]
        if environ.get('REQUEST_METHOD')=='GET' and environ.get('PATH_INFO')=='/live':return reply(200,{'status':'alive'})
        if environ.get('REQUEST_METHOD')=='GET' and environ.get('PATH_INFO')=='/health':
            try:
                ready=self.store.readiness(now=self.clock())
                venues=tuple(sorted({b.venue_id for b in self.bindings.values()}))
                catalog_ready=True
                if self.catalog is not None:
                    catalog_ready=all(self.catalog.has_minimum_inventory(v,3) for v in venues)
                return reply(200 if ready['ready'] and catalog_ready else 503,
                             {'status':'ready' if ready['ready'] and catalog_ready else 'degraded',
                              'catalog_ready':catalog_ready})
            except Exception:
                # Catalog adapters may raise provider-specific database exception
                # classes (e.g. psycopg2.Error); never leak a stack trace here.
                return reply(503,{'status':'unavailable','ready':False})
        if environ.get('REQUEST_METHOD')!='POST':return reply(405,{'error':'method_not_allowed'})
        path=environ.get('PATH_INFO','')
        if not path.startswith('/webhook/') or path.count('/')!=2:return reply(404,{'error':'not_found'})
        binding=self.bindings.get(path[len('/webhook/'):])
        if binding is None:return reply(404,{'error':'not_found'})
        if environ.get('CONTENT_TYPE','').split(';')[0].lower()!='application/json':return reply(415,{'error':'invalid_content_type'})
        length_str=environ.get('CONTENT_LENGTH','')
        if not length_str or not length_str.isdigit():return reply(400,{'error':'missing_length'})
        if len(length_str)>8:return reply(413,{'error':'too_large'})
        length=int(length_str)
        if length>MAX_BODY_BYTES:return reply(413,{'error':'too_large'})
        try:
            raw=environ['wsgi.input'].read(length)
            if len(raw)!=length:return reply(400,{'error':'incomplete_body'})
            observed=self.clock()
            request=verify_and_parse(raw,environ.get('HTTP_TYPEFORM_SIGNATURE',''),binding,now=observed)
            outcome=self.store.enqueue_request(token=request.token,venue=request.venue_id,recipient=request.recipient,preferences=request.preferences,now=observed)
        except PermissionError:return reply(401,{'error':'unauthorized'})
        except TokenCollision:return reply(400,{'error':'token_collision'})
        except InputError:return reply(400,{'error':'invalid_request'})
        except (sqlite3.Error,OSError,RuntimeError,ValueError):return reply(503,{'error':'temporarily_unavailable'})
        return reply(429 if outcome=='rate_limited' else (202 if outcome=='queued' else 200),{'status':outcome})
