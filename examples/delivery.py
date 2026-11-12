"""Durable, at-most-one-automatic-submission demonstration.

A remote email service cannot generally guarantee exactly-once delivery. Once a
submission MAY have reached the provider, uncertainty is held for reconciliation
rather than retried. All state transitions are committed before side effects.
"""
from __future__ import annotations
import hashlib
import json
import math
import re
import unicodedata
import sqlite3
import time
import threading
import uuid
from dataclasses import asdict,dataclass
from pathlib import Path
from typing import Callable

from dbutil import SQLiteDatabase,assert_canonical_table
from pipeline import (CatalogDataError,ContractError,InputError,InsufficientCandidates,valid_venue_id,Selection,
                      Preferences,create_deterministic_recommendations,load_catalog,
                      select_candidates,validate_recommendation_contract,validate_selection_priority)

MAX_DELIVERY_ATTEMPTS=3
BASE_RETRY_DELAY_SECONDS=30.0
MAX_QUEUED_EVENTS=50000
MAX_PER_VENUE_HOUR=500
MAX_PER_VENUE_DAY=2500
MAX_PER_RECIPIENT_DAY=5
MODEL_ALERT_WINDOW_SECONDS=3600
MAX_LEASE_SECONDS=300.0
HEARTBEAT_INTERVAL_SECONDS=8.0
MAX_RECONCILIATION_AGE_SECONDS=90*86400
CAPACITY_STATES=('pending','retryable_failure','sending','submitting','reconciliation_required')
MAX_STORED_REQUEST_BYTES=200_000
TERMINAL_STATES=frozenset(('provider_accepted','permanent_failure','retry_exhausted','reconciled_cancelled'))

class TokenCollision(ValueError):pass
class KnownNotAccepted(Exception):
    """Verified negative provider response; no email was accepted (safe to retry)."""
    def __init__(self,message='Provider did not accept',*,retry_after:float|None=None):
        super().__init__(message)
        self.retry_after=retry_after
class PermanentProviderReject(Exception):
    """Provider definitely rejected the submission permanently."""
class SubmissionUncertain(Exception):
    """Timeout, connection reset or unknown acceptance state; NEVER auto-retry."""

@dataclass(frozen=True)
class Claim:
    status:str
    attempt:int
    nonce:str|None=None

def _time(now:float)->float:
    if isinstance(now,bool) or not isinstance(now,(float,int)) or not math.isfinite(now) or now<0:
        raise ValueError('Timestamp must be a finite nonnegative UTC epoch second')
    return float(now)

def normalize_recipient(recipient:str,*,require_email:bool=False)->str:
    if not isinstance(recipient,str) or len(recipient)>254:raise InputError('Invalid recipient')
    value=recipient.strip()
    if '@' in value:
        local,domain=value.rsplit('@',1)
        value=local+'@'+domain.lower()
    if not value or any(c.isspace() for c in value) or any(ord(c)<33 for c in value):raise InputError('Invalid recipient')
    if require_email and not re.fullmatch(r'[A-Za-z0-9.!#$%&\'*+/=?^_`{|}~-]+@[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+',value):
        raise InputError('Recipient must be a valid email address')
    if require_email:
        local,domain=value.rsplit('@',1)
        if len(local)>64 or local.startswith('.') or local.endswith('.') or '..' in local:
            raise InputError('Invalid unquoted email local part')
        if any(len(part)>63 for part in domain.split('.')):
            raise InputError('Email DNS label too long')
    return value

def fingerprint_for(venue:str,recipient:str,preferences:Preferences)->str:
    if not isinstance(preferences,Preferences):raise InputError('Invalid preferences')
    document={'venue':venue,'recipient':normalize_recipient(recipient),'preferences':asdict(preferences)}
    return hashlib.sha256(json.dumps(document,sort_keys=True,separators=(',',':')).encode()).hexdigest()

class EventStore:
    def __init__(self,path:Path|str):
        self._db=SQLiteDatabase(path)
        self.path=str(path)
        with self._connect() as conn:
            schema_script='''
             CREATE TABLE IF NOT EXISTS events (
              token TEXT PRIMARY KEY CHECK(length(token)>0 AND length(token)<=128),
              fingerprint TEXT NOT NULL, request_json TEXT,
              state TEXT NOT NULL CHECK(state IN ('pending','sending','submitting','retryable_failure',
                'provider_accepted','permanent_failure','retry_exhausted','reconciliation_required','reconciled_cancelled')),
              attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0),
              cycle_attempts INTEGER NOT NULL DEFAULT 0 CHECK(cycle_attempts>=0),
              lease_until REAL,next_retry_at REAL,delivery_ref TEXT,last_error TEXT,
              created_at REAL NOT NULL,updated_at REAL NOT NULL,dispatch_nonce TEXT);
             CREATE INDEX IF NOT EXISTS events_due ON events(state,next_retry_at);
             CREATE INDEX IF NOT EXISTS events_retention ON events(state,updated_at);
             CREATE TABLE IF NOT EXISTS event_tombstones (
              token_hash TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,state TEXT NOT NULL,archived_at REAL NOT NULL);
             CREATE INDEX IF NOT EXISTS tombstone_retention ON event_tombstones(archived_at);
             CREATE TABLE IF NOT EXISTS event_audit (
              id INTEGER PRIMARY KEY AUTOINCREMENT,token_hash TEXT NOT NULL,attempt INTEGER NOT NULL,
              state TEXT NOT NULL,at REAL NOT NULL,category TEXT);
             CREATE INDEX IF NOT EXISTS event_audit_age ON event_audit(at);
             CREATE TABLE IF NOT EXISTS worker_status (
              worker_id TEXT PRIMARY KEY,last_poll_at REAL NOT NULL);
             CREATE TABLE IF NOT EXISTS admission_ledger (
              token TEXT PRIMARY KEY REFERENCES events(token) ON DELETE CASCADE,
              venue TEXT NOT NULL, recipient_hash TEXT NOT NULL, admitted_at REAL NOT NULL);
             CREATE INDEX IF NOT EXISTS admission_venue_at ON admission_ledger(venue,admitted_at);
             CREATE INDEX IF NOT EXISTS admission_recipient_at ON admission_ledger(recipient_hash,admitted_at);
             CREATE TABLE IF NOT EXISTS admission_denials (
              venue TEXT NOT NULL,reason TEXT NOT NULL,window_start INTEGER NOT NULL,
              denied_count INTEGER NOT NULL DEFAULT 1,
              PRIMARY KEY (venue,reason,window_start));
             CREATE INDEX IF NOT EXISTS admission_denials_window ON admission_denials(window_start);
             CREATE TABLE IF NOT EXISTS event_history (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              token TEXT NOT NULL REFERENCES events(token) ON DELETE CASCADE,
              attempt INTEGER NOT NULL,state TEXT NOT NULL,at REAL NOT NULL,category TEXT);
            '''
            definitions={}
            for statement in schema_script.split(';'):
                match=re.search(r'CREATE\s+(TABLE|INDEX)\s+IF\s+NOT\s+EXISTS\s+(\w+)',statement,re.I)
                if match:
                    definitions[match.group(2)]=(match.group(1).lower(),statement)
            def canonical(sql):
                return re.sub(r'\s+','',re.sub(r'\bIF\s+NOT\s+EXISTS\b','',sql,flags=re.I)).lower()
            objects={name:(kind,ddl) for kind,name,ddl in conn.execute(
                "SELECT type,name,sql FROM sqlite_master WHERE type IN ('table','index','trigger')")
                if not name.startswith('sqlite_autoindex_') and name not in
                ('sqlite_sequence','__palate_store_identity')}
            unknown=objects.keys()-definitions.keys()
            if unknown:
                raise ValueError('Unsafe unexpected SQLite delivery schema objects: '+', '.join(sorted(unknown)))
            for name,(kind,ddl) in objects.items():
                expected_kind,expected_ddl=definitions[name]
                if kind!=expected_kind or ddl is None or canonical(ddl)!=canonical(expected_ddl):
                    raise ValueError('Unsafe historical SQLite schema drift: '+name)
            # Partially initialized durable state is not a fresh database. A
            # missing ledger or index must not silently bypass provenance.
            if objects and objects.keys()!=definitions.keys():
                raise ValueError('Unsafe incomplete historical SQLite delivery schema')
            conn.executescript(schema_script)
            for name,(kind,ddl) in definitions.items():
                item=conn.execute('SELECT type,sql FROM sqlite_master WHERE name=?',(name,)).fetchone()
                if item is None or item[0]!=kind or canonical(item[1])!=canonical(ddl):
                    raise ValueError('Unsafe SQLite delivery schema: '+name)
            violations=conn.execute('PRAGMA foreign_key_check').fetchall()
            if violations:
                raise ValueError('SQLite delivery foreign-key violations detected')
    def close(self):self._db.close()
    def _connect(self):return self._db.connect()
    @staticmethod
    def _active_count(conn):
        return conn.execute("SELECT count(*) FROM events WHERE state IN ('pending','retryable_failure','sending','submitting','reconciliation_required')").fetchone()[0]
    @staticmethod
    def _check_token(token):
        if (type(token) is not str or not 1<=len(token)<=128 or token!=token.strip() or
            any(ord(ch)<33 or ord(ch)==127 or unicodedata.category(ch) in ('Cc','Cf','Cs') for ch in token)):
            raise InputError('Invalid token')
        try:
            token.encode('utf-8','strict')
        except UnicodeError as error:
            raise InputError('Invalid token encoding') from error
    @staticmethod
    def _history(conn,token,attempt,state,now,category=None):
        conn.execute('INSERT INTO event_history(token,attempt,state,at,category) VALUES(?,?,?,?,?)',(token,attempt,state,now,category))
        conn.execute('INSERT INTO event_audit(token_hash,attempt,state,at,category) VALUES(?,?,?,?,?)',
                     (hashlib.sha256(token.encode()).hexdigest(),attempt,state,now,category))
    def enqueue_request(self,*,token:str,venue:str,recipient:str,preferences:Preferences,now:float)->str:
        """Persist verified and normalized request so worker can retry without redelivery."""
        now=_time(now);self._check_token(token)
        if not valid_venue_id(venue):raise InputError('Invalid venue')
        recipient=normalize_recipient(recipient,require_email=True)
        fingerprint=fingerprint_for(venue,recipient,preferences)
        payload=json.dumps({'venue':venue,'recipient':recipient,'preferences':asdict(preferences)},sort_keys=True,separators=(',',':'))
        with self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            existing=conn.execute('SELECT fingerprint FROM events WHERE token=?',(token,)).fetchone()
            if not existing:
                archived=conn.execute('SELECT fingerprint FROM event_tombstones WHERE token_hash=?',(hashlib.sha256(token.encode()).hexdigest(),)).fetchone()
                if archived:
                    if archived[0]!=fingerprint:raise TokenCollision('Archived token collision')
                    return 'already_queued'
            if existing:
                if existing[0]!=fingerprint:raise TokenCollision('Token belongs to another request')
                return 'already_queued'
            if self._active_count(conn)>=MAX_QUEUED_EVENTS:raise RuntimeError('Queue capacity reached')
            # Counts are atomic under BEGIN IMMEDIATE, excluding duplicate tokens.
            # Quota identity is intentionally case-folded; delivery retains the
            # original SMTP local-part. This blocks trivial case-variation abuse
            # without changing the actual recipient address.
            quota_identity=recipient.casefold()
            recipient_hash=hashlib.sha256(quota_identity.encode()).hexdigest()
            checks=(
                ('venue_hour',"SELECT count(*) FROM admission_ledger WHERE venue=? AND admitted_at>?",(venue,now-3600),MAX_PER_VENUE_HOUR),
                ('venue_day',"SELECT count(*) FROM admission_ledger WHERE venue=? AND admitted_at>?",(venue,now-86400),MAX_PER_VENUE_DAY),
                ('recipient_day',"SELECT count(*) FROM admission_ledger WHERE recipient_hash=? AND admitted_at>?",(recipient_hash,now-86400),MAX_PER_RECIPIENT_DAY),
            )
            exceeded=next((code for code,sql,params,bound in checks if conn.execute(sql,params).fetchone()[0]>=bound),None)
            if exceeded:
                conn.execute('''INSERT INTO admission_denials(venue,reason,window_start,denied_count) VALUES(?,?,?,1)
                              ON CONFLICT(venue,reason,window_start) DO UPDATE SET denied_count=denied_count+1''',(venue,exceeded,int(now//3600)*3600))
                # Save the denial for operators without admitting another email.
                return 'rate_limited'
            conn.execute("INSERT INTO events(token,fingerprint,request_json,state,created_at,updated_at) VALUES(?,?,?,'pending',?,?)",(token,fingerprint,payload,now,now))
            conn.execute('INSERT INTO admission_ledger(token,venue,recipient_hash,admitted_at) VALUES(?,?,?,?)',(token,venue,recipient_hash,now))
            self._history(conn,token,0,'pending',now)
        return 'queued'
    def due_requests(self,*,now:float,limit:int=50)->tuple[tuple[str,dict],...]:
        now=_time(now)
        if type(limit) is not int or limit<1 or limit>500:raise InputError('Invalid batch limit')
        with self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            # Any orphan pre-submission request must be quarantined, not left in
            # a state that appears schedulable without a persisted payload.
            orphans=conn.execute("SELECT token,attempts FROM events WHERE request_json IS NULL AND state IN ('pending','retryable_failure','sending')").fetchall()
            for token,attempt in orphans:
                conn.execute("UPDATE events SET state='permanent_failure',last_error='missing_durable_request',updated_at=?,lease_until=NULL,next_retry_at=NULL WHERE token=?",(now,token))
                self._history(conn,token,attempt,'permanent_failure',now,'missing_durable_request')
            rows=conn.execute("""SELECT token,request_json,fingerprint FROM events WHERE request_json IS NOT NULL AND
             (state='pending' OR (state='retryable_failure' AND next_retry_at<=?) OR (state='sending' AND lease_until<=?))
             ORDER BY created_at,token LIMIT ?""",(now,now,limit)).fetchall()
            valid=[]
            for token,payload,fingerprint in rows:
                try:
                    if type(payload) is not str or len(payload)>MAX_STORED_REQUEST_BYTES:
                        raise ValueError('Stored request too large')
                    decoded=json.loads(payload)
                    if type(decoded) is not dict or set(decoded)!={'venue','recipient','preferences'}:
                        raise ValueError('Stored request shape invalid')
                    venue=decoded['venue']
                    if not valid_venue_id(venue):raise ValueError('Invalid persisted venue')
                    recipient=normalize_recipient(decoded['recipient'],require_email=True)
                    prefs=Preferences(**decoded['preferences'])
                    if fingerprint_for(venue,recipient,prefs)!=fingerprint:
                        raise ValueError('Stored request fingerprint invalid')
                    valid.append((token,decoded))
                except (ValueError,TypeError,UnicodeError,RecursionError):
                    attempt=conn.execute('SELECT attempts FROM events WHERE token=?',(token,)).fetchone()[0]
                    conn.execute("UPDATE events SET state='permanent_failure',request_json=NULL,last_error='corrupt_stored_json',updated_at=?,lease_until=NULL,next_retry_at=NULL WHERE token=?",(now,token))
                    self._history(conn,token,attempt,'permanent_failure',now,'corrupt_stored_json')
            return tuple(valid)
    def poison(self,token:str,*,now:float,reason:str='invalid_stored_request')->None:
        now=_time(now);self._check_token(token)
        with self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            row=conn.execute("SELECT attempts,state,lease_until FROM events WHERE token=?",(token,)).fetchone()
            if row and row[1]=='sending' and (row[2] is None or row[2]>now):
                raise RuntimeError('Active worker claim cannot be poisoned without its claim fence')
            # Once the pre-submit lease expired, no provider dispatch may begin
            # under this nonce; quarantine atomically and invalidate that claim.
            if row and (row[1] in ('pending','retryable_failure') or
                        (row[1]=='sending' and row[2] is not None and row[2]<=now)):
                conn.execute("UPDATE events SET state='permanent_failure',request_json=NULL,lease_until=NULL,next_retry_at=NULL,last_error=?,updated_at=? WHERE token=?",(reason,now,token))
                self._history(conn,token,row[0],'permanent_failure',now,reason)
    def worker_heartbeat(self,*,now:float,worker_id:str='default')->None:
        now=_time(now)
        if type(worker_id) is not str or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,64}',worker_id):
            raise InputError('Worker ID must be a bounded simple identifier')
        with self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            conn.execute('DELETE FROM worker_status WHERE last_poll_at<?',(now-86400,))
            present=conn.execute('SELECT 1 FROM worker_status WHERE worker_id=?',(worker_id,)).fetchone()
            if not present and conn.execute('SELECT count(*) FROM worker_status').fetchone()[0]>=1024:
                raise InputError('Worker heartbeat cardinality limit exceeded')
            conn.execute('INSERT INTO worker_status(worker_id,last_poll_at) VALUES(?,?) ON CONFLICT(worker_id) DO UPDATE SET last_poll_at=excluded.last_poll_at',(worker_id,now))
    def readiness(self,*,now:float,worker_stale_after:float=120.0)->dict:
        now=_time(now)
        with self._connect() as conn:
            conn.execute('SELECT 1').fetchone()
            queued=self._active_count(conn)
            latest=conn.execute('SELECT max(last_poll_at) FROM worker_status').fetchone()[0]
        worker_ok=latest is not None and 0<=now-latest<=worker_stale_after
        return {'status':'ready' if worker_ok and queued<MAX_QUEUED_EVENTS else 'degraded',
                'ready':bool(worker_ok and queued<MAX_QUEUED_EVENTS),'worker_recent':worker_ok,
                'queued_count':queued}
    def alerts(self,*,now:float,age_threshold:float=3600,queue_age_threshold:float=900)->tuple[dict,...]:
        now=_time(now)
        with self._connect() as conn:
            unresolved=conn.execute("SELECT count(*),min(updated_at) FROM events WHERE state IN ('reconciliation_required','retry_exhausted','permanent_failure')").fetchone()
            queue=conn.execute("SELECT count(*),min(created_at) FROM events WHERE state IN ('pending','retryable_failure')").fetchone()
        output=[]
        if unresolved[0] and now-unresolved[1]>=age_threshold:output.append({'code':'unresolved_delivery','count':unresolved[0]})
        if queue[0] and now-queue[1]>=queue_age_threshold:output.append({'code':'stale_queue','count':queue[0]})
        with self._connect() as conn:
            config=conn.execute("SELECT count(*) FROM event_audit WHERE category IN ('model_degraded:configuration','model_degraded:contract') AND at>=?",(now-MODEL_ALERT_WINDOW_SECONDS,)).fetchone()[0]
            transient=conn.execute("SELECT count(*) FROM event_audit WHERE category='model_degraded:transient' AND at>=?",(now-MODEL_ALERT_WINDOW_SECONDS,)).fetchone()[0]
            denials=conn.execute('SELECT COALESCE(sum(denied_count),0) FROM admission_denials WHERE window_start>=?',(int((now-MODEL_ALERT_WINDOW_SECONDS)//3600)*3600,)).fetchone()[0]
        if config:output.append({'code':'model_configuration_degraded','count':config})
        if transient>=3:output.append({'code':'model_transient_degraded','count':transient})
        if denials:output.append({'code':'admission_quota_exceeded','count':denials})
        return tuple(output)
    @staticmethod
    def _require_nonce(nonce: str | None) -> str:
        if (type(nonce) is not str or len(nonce)!=32 or
            any(ch not in '0123456789abcdef' for ch in nonce)):
            raise RuntimeError('A valid claim dispatch nonce is required')
        return nonce

    def record_model_degradation(self,token:str,attempt:int,*,now:float,category:str,nonce:str|None=None)->None:
        self._require_nonce(nonce)
        now=_time(now)
        if category not in ('configuration','contract','transient'):
            raise ValueError('Unknown degradation category')
        with self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            row=conn.execute("SELECT 1 FROM events WHERE token=? AND attempts=? AND state='sending' AND dispatch_nonce=?",(token,attempt,nonce)).fetchone()
            if not row:raise RuntimeError('Stale model audit attempt')
            self._history(conn,token,attempt,'sending',now,'model_degraded:'+category)
    @staticmethod
    def _claim_has_verified_admission(conn,token:str,fingerprint:str)->bool:
        record=conn.execute('SELECT request_json FROM events WHERE token=?',(token,)).fetchone()
        ledger=conn.execute('SELECT venue,recipient_hash FROM admission_ledger WHERE token=?',(token,)).fetchone()
        if not record or not ledger or type(record[0]) is not str or len(record[0])>MAX_STORED_REQUEST_BYTES:
            return False
        try:
            payload=json.loads(record[0])
            if type(payload) is not dict or set(payload)!= {'venue','recipient','preferences'}:
                return False
            venue=payload['venue'];recipient=normalize_recipient(payload['recipient'],require_email=True)
            preferences=Preferences(**payload['preferences'])
            return (valid_venue_id(venue) and venue==ledger[0] and
                    hashlib.sha256(recipient.casefold().encode()).hexdigest()==ledger[1] and
                    fingerprint_for(venue,recipient,preferences)==fingerprint)
        except (TypeError,ValueError,KeyError,UnicodeError,RecursionError):
            return False

    def claim(self,token:str,fingerprint:str,*,now:float,lease_seconds:float=30.0)->Claim:
        now=_time(now);lease_seconds=_time(lease_seconds);self._check_token(token)
        if not isinstance(fingerprint,str) or not fingerprint or not 0<lease_seconds<=MAX_LEASE_SECONDS:raise InputError('Invalid claim')
        with self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            existing=conn.execute('SELECT fingerprint,state,lease_until,attempts,next_retry_at,cycle_attempts FROM events WHERE token=?',(token,)).fetchone()
            if existing is None:
                archived=conn.execute('SELECT fingerprint,state FROM event_tombstones WHERE token_hash=?',(hashlib.sha256(token.encode()).hexdigest(),)).fetchone()
                if archived:
                    if archived[0]!=fingerprint:raise TokenCollision('Archived token collision')
                    return Claim('already_accepted' if archived[1]=='provider_accepted' else archived[1],0)
                raise InputError('Claim requires a verified durable admission')
            stored,state,lease_until,attempts,next_retry_at,cycle_attempts=existing
            if stored!=fingerprint:raise TokenCollision('Token already belongs to a different request')
            if not self._claim_has_verified_admission(conn,token,fingerprint):
                raise InputError('Claim requires a verified durable request and admission ledger')
            if state=='provider_accepted':return Claim('already_accepted',attempts)
            if state in TERMINAL_STATES:return Claim(state,attempts)
            if state=='reconciliation_required':return Claim(state,attempts)
            if state=='submitting':
                # Remote side effect may have happened; do not dispatch again.
                if lease_until is not None and lease_until>now:return Claim('in_flight',attempts)
                conn.execute("UPDATE events SET state='reconciliation_required',lease_until=NULL,updated_at=?,last_error='uncertain_submit' WHERE token=?",(now,token))
                self._history(conn,token,attempts,'reconciliation_required',now,'uncertain_submit')
                return Claim('reconciliation_required',attempts)
            if state=='sending' and lease_until is not None and lease_until>now:return Claim('in_flight',attempts)
            if state=='retryable_failure' and next_retry_at is not None and next_retry_at>now:return Claim('retry_scheduled',attempts)
            if cycle_attempts>=MAX_DELIVERY_ATTEMPTS:
                conn.execute("UPDATE events SET state='retry_exhausted',lease_until=NULL,next_retry_at=NULL,updated_at=? WHERE token=?",(now,token))
                self._history(conn,token,attempts,'retry_exhausted',now)
                return Claim('retry_exhausted',attempts)
            nonce=uuid.uuid4().hex
            conn.execute("UPDATE events SET state='sending',attempts=attempts+1,cycle_attempts=cycle_attempts+1,dispatch_nonce=?,lease_until=?,next_retry_at=NULL,updated_at=? WHERE token=?",(nonce,now+lease_seconds,now,token))
            self._history(conn,token,attempts+1,'sending',now)
            return Claim('claimed',attempts+1,nonce)
    def heartbeat(self,token:str,attempt:int,*,now:float,lease_seconds:float=30.,nonce:str|None=None)->None:
        self._require_nonce(nonce)
        now=_time(now)
        if type(lease_seconds) not in (int,float) or not math.isfinite(lease_seconds) or not 0<lease_seconds<=MAX_LEASE_SECONDS:
            raise InputError('Lease extension must be positive and bounded')
        with self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            row=conn.execute("UPDATE events SET lease_until=?,updated_at=? WHERE token=? AND attempts=? AND state IN ('sending','submitting') AND lease_until>? AND dispatch_nonce=?",(now+lease_seconds,now,token,attempt,now,nonce)).rowcount
            if row!=1:raise RuntimeError('Cannot heartbeat stale worker')
    def mark_dispatch_started(self,token:str,attempt:int,*,now:float,nonce:str|None=None)->None:
        self._require_nonce(nonce)
        now=_time(now)
        with self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            row=conn.execute("UPDATE events SET state='submitting',updated_at=? WHERE token=? AND attempts=? AND state='sending' AND lease_until>? AND dispatch_nonce=?",(now,token,attempt,now,nonce)).rowcount
            if row!=1:raise RuntimeError('Expired or stale claim; dispatch forbidden')
            self._history(conn,token,attempt,'submitting',now)
    def _cycle_count(self,token,attempt,nonce=None):
        self._require_nonce(nonce)
        with self._connect() as conn:
            row=conn.execute('SELECT cycle_attempts FROM events WHERE token=? AND attempts=? AND dispatch_nonce=?',
                             (token,attempt,nonce)).fetchone()
        if row is None or row[0]<1:
            raise RuntimeError('Stale claim or invalid retry cycle')
        return row[0]

    def _transition(self,token,attempt,from_state,to_state,now,*,ref=None,error=None,retry_at=None,nonce=None):
        self._require_nonce(nonce)
        now=_time(now)
        with self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            changed=conn.execute('''UPDATE events SET state=?,delivery_ref=COALESCE(?,delivery_ref),lease_until=NULL,
               next_retry_at=?,last_error=?,updated_at=? WHERE token=? AND attempts=? AND state=? AND dispatch_nonce=?''',
               (to_state,ref,retry_at,error,now,token,attempt,from_state,nonce)).rowcount
            if changed!=1:raise RuntimeError('Stale attempt or unexpected state transition')
            self._history(conn,token,attempt,to_state,now,error)
    def mark_provider_accepted(self,token:str,attempt:int,provider_ref:str,*,now:float,nonce:str|None=None)->None:
        if not isinstance(provider_ref,str) or not provider_ref.strip() or len(provider_ref)>255:raise ValueError('Invalid provider reference')
        self._require_nonce(nonce)
        now=_time(now)
        with self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            row=conn.execute('''SELECT state,last_error FROM events
                WHERE token=? AND attempts=? AND dispatch_nonce=?''',(token,attempt,nonce)).fetchone()
            if row is None or (row[0]!='submitting' and
                 not (row[0]=='reconciliation_required' and
                      row[1] in ('expired_submission','uncertain_submit'))):
                raise RuntimeError('Stale submission or invalid provider acceptance fence')
            # Both the reaper and a competing claimant can hold an expired
            # dispatch. The original nonce+attempt is the acceptance fence;
            # never reopen cancellations or a later independently claimed send.
            late=row[0]=='reconciliation_required'
            changed=conn.execute('''UPDATE events SET state='provider_accepted',delivery_ref=?,lease_until=NULL,
                next_retry_at=NULL,last_error=?,updated_at=?
                WHERE token=? AND attempts=? AND dispatch_nonce=? AND state=?''',
                (provider_ref,'late_accepted' if late else None,now,token,attempt,nonce,row[0])).rowcount
            if changed!=1:raise RuntimeError('Acceptance fence changed during transition')
            self._history(conn,token,attempt,'provider_accepted',now,'late_accepted' if late else None)
    def mark_uncertain(self,token:str,attempt:int,*,now:float,reason:str='uncertain_submit',nonce:str|None=None)->None:
        self._require_nonce(nonce)
        if nonce is not None:
            # A reaper might have already moved this SAME dispatch to a
            # reconciliation hold during a delayed HTTP call. The state is
            # already safe and must not produce a worker crash.
            with self._connect() as conn:
                row=conn.execute('SELECT state,last_error FROM events WHERE token=? AND attempts=? AND dispatch_nonce=?',
                                 (token,attempt,nonce)).fetchone()
            if row and row[0]=='reconciliation_required' and row[1] in ('expired_submission','uncertain_submit'):
                return
        self._transition(token,attempt,'submitting','reconciliation_required',now,error=reason,nonce=nonce)
    def mark_retryable_failure(self,token:str,attempt:int,*,now:float,reason:str='temporary_pre_send',nonce:str|None=None)->str:
        dest='retry_exhausted' if self._cycle_count(token,attempt,nonce)>=MAX_DELIVERY_ATTEMPTS else 'retryable_failure'
        delay=None if dest=='retry_exhausted' else _time(now)+BASE_RETRY_DELAY_SECONDS*2**(self._cycle_count(token,attempt,nonce)-1)
        self._transition(token,attempt,'sending',dest,now,error=reason,retry_at=delay,nonce=nonce)
        return dest
    def mark_known_not_accepted(self,token:str,attempt:int,*,now:float,retry_after:float|None=None,nonce:str|None=None)->str:
        # A documented provider rejection proved no acceptance; move out of submitting atomically.
        dest='retry_exhausted' if self._cycle_count(token,attempt,nonce)>=MAX_DELIVERY_ATTEMPTS else 'retryable_failure'
        if retry_after is not None and (type(retry_after) not in (int,float) or not math.isfinite(retry_after) or retry_after<0):
            retry_after=None
        backoff=BASE_RETRY_DELAY_SECONDS*2**(self._cycle_count(token,attempt,nonce)-1)
        delay=None if dest=='retry_exhausted' else _time(now)+min(86400.0,max(backoff,retry_after or 0.0))
        self._transition(token,attempt,'submitting',dest,now,error='provider_not_accepted',retry_at=delay,nonce=nonce)
        return dest
    def mark_permanent_failure(self,token:str,attempt:int,*,now:float,reason:str='request_unfulfillable',from_state:str='sending',nonce:str|None=None)->None:
        self._transition(token,attempt,from_state,'permanent_failure',now,error=reason,nonce=nonce)
    def summary(self,token:str)->tuple[str,int,str|None]|None:
        with self._connect() as conn:
            return conn.execute('SELECT state,attempts,delivery_ref FROM events WHERE token=?',(token,)).fetchone()
    def history(self,token:str)->tuple[tuple,...]:
        with self._connect() as conn:
            return tuple(conn.execute('SELECT attempt,state,at,category FROM event_history WHERE token=? ORDER BY id',(token,)).fetchall())
    def unresolved(self,limit:int=100)->tuple[tuple,...]:
        if type(limit) is not int or not 1<=limit<=500:
            raise ValueError('Unresolved query limit must be between 1 and 500')
        with self._connect() as conn:
            return tuple(conn.execute("SELECT token,state,attempts,last_error,updated_at FROM events WHERE state IN ('reconciliation_required','retry_exhausted','permanent_failure') ORDER BY updated_at LIMIT ?",(limit,)).fetchall())
    def reconcile(self,token:str,*,now:float,decision:str,evidence:str,
                  provider_ref:str|None=None,receipt_verifier:Callable|None=None)->None:
        """Operator-only action. Confirmed acceptance requires a verifiable provider receipt.

        The network verification occurs BEFORE the SQLite write transaction. The
        event snapshot is then rechecked under BEGIN IMMEDIATE to prevent a
        concurrent operator from switching its state while verification runs.
        """
        now=_time(now)
        if not evidence or not isinstance(evidence,str) or len(evidence.strip())<12 or len(evidence)>500:
            raise ValueError('Documented reconciliation evidence required')
        if decision not in ('confirmed_accepted','confirmed_not_sent','cancel'):raise ValueError('Invalid reconciliation decision')
        if decision=='confirmed_accepted' and (not isinstance(provider_ref,str) or not provider_ref.strip() or len(provider_ref)>255):
            raise ValueError('Verified provider reference required for confirmed acceptance')
        verified_snapshot=None
        if decision=='confirmed_accepted':
            with self._connect() as conn:
                verified_snapshot=conn.execute('SELECT state,attempts,request_json,fingerprint FROM events WHERE token=?',(token,)).fetchone()
            if not verified_snapshot or verified_snapshot[0]!='reconciliation_required':
                raise ValueError('Cannot confirm provider acceptance: no submission is unresolved')
            if not callable(receipt_verifier):
                raise ValueError('Authenticated provider receipt verifier required')
            try:
                persisted=json.loads(verified_snapshot[2])
                recipient=normalize_recipient(persisted['recipient'],require_email=True)
                if not callable(receipt_verifier) or receipt_verifier(provider_ref,recipient,hashlib.sha256(token.encode()).hexdigest()) is not True:
                    raise ValueError('Provider receipt does not verify the event and recipient')
            except (ValueError,TypeError,KeyError,RecursionError) as exc:
                raise ValueError('Provider receipt verification failed') from exc
        with self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            row=conn.execute('SELECT state,attempts,request_json,fingerprint FROM events WHERE token=?',(token,)).fetchone()
            if not row or row[0] not in ('reconciliation_required','retry_exhausted','permanent_failure'):
                raise ValueError('Event is not eligible for reconciliation')
            state,attempts,payload,fingerprint=row
            if decision=='confirmed_accepted' and row!=verified_snapshot:
                raise ValueError('Event changed while provider receipt was verified')
            if decision=='confirmed_accepted':
                # Ordinary reconciliation only applies to the *current unresolved*
                # potentially-accepted dispatch, never to an earlier explicitly
                # rejected attempt or a permanently failed pre-submit request.
                if state!='reconciliation_required':
                    raise ValueError('Cannot confirm provider acceptance: no submission is unresolved')
                last=conn.execute('SELECT state FROM event_history WHERE token=? AND attempt=? ORDER BY id DESC LIMIT 1',(token,attempts)).fetchone()
                dispatched=conn.execute("SELECT 1 FROM event_history WHERE token=? AND attempt=? AND state='submitting' LIMIT 1",(token,attempts)).fetchone()
                if not dispatched or not last or last[0]!='reconciliation_required':
                    raise ValueError('Cannot confirm provider acceptance: no unresolved dispatch')
            if decision=='confirmed_not_sent':
                # A provider submission (even one that timed out) can never be
                # proved absent by a free-text assertion. Forbid requeueing any
                # event that ever entered 'submitting'. Safe, known-negative
                # provider failures already use the bounded automatic retry path.
                # Operators can cancel, or confirm acceptance with a real receipt.
                submitting=conn.execute("SELECT attempt FROM event_history WHERE token=? AND state='submitting' ORDER BY id",(token,)).fetchall()
                # Only proven-negative HTTP rejection / bounded known-negative
                # transitions may close a submission. Arbitrary operator text,
                # uncertainty or merely checking a dashboard never suffices.
                proven_negative=(state=='permanent_failure' and
                    conn.execute("SELECT last_error FROM events WHERE token=?",(token,)).fetchone()[0]=='provider_definitely_rejected')
                if proven_negative:
                    for (submission_attempt,) in submitting:
                        known=conn.execute("""SELECT state,category FROM event_history
                            WHERE token=? AND attempt=? AND
                            ((state IN ('retryable_failure','retry_exhausted') AND category='provider_not_accepted') OR
                             (state='permanent_failure' AND category='provider_definitely_rejected'))
                            ORDER BY id DESC LIMIT 1""",(token,submission_attempt)).fetchone()
                        if known is None:
                            proven_negative=False
                            break
                if state=='reconciliation_required' or (submitting and not proven_negative):
                    raise ValueError('Cannot requeue: provider submission may have been accepted')
                # Scrubbed or poisoned events must never be requeued without a
                # validated authenticated request; operator may cancel/confirm.
                if payload is None or len(payload)>MAX_STORED_REQUEST_BYTES:
                    raise ValueError('Cannot requeue: verified request payload unavailable')
                try:
                    decoded=json.loads(payload)
                    if not isinstance(decoded,dict) or set(decoded)!={'venue','recipient','preferences'}:
                        raise ValueError('Stored request invalid')
                    normalized=Preferences(**decoded['preferences'])
                    expected=fingerprint_for(decoded['venue'],normalize_recipient(decoded['recipient'],require_email=True),normalized)
                    if expected!=fingerprint:raise ValueError('Stored request fingerprint mismatch')
                except (ValueError,TypeError,KeyError,RecursionError,InputError) as error:
                    raise ValueError('Cannot requeue: corrupt request; must reconstruct and reauthenticate') from error
            next_state={'confirmed_accepted':'provider_accepted','confirmed_not_sent':'pending','cancel':'reconciled_cancelled'}[decision]
            # Reissued preverified unsent event gets a new retry budget; operator decision is audited.
            if decision=='confirmed_accepted':
                conn.execute('UPDATE events SET state=?,updated_at=?,lease_until=NULL,delivery_ref=?,last_error=? WHERE token=?',(next_state,now,provider_ref,'operator_confirmed_accepted',token))
            else:
                conn.execute('UPDATE events SET state=?,updated_at=?,cycle_attempts=?,lease_until=NULL,next_retry_at=NULL,last_error=? WHERE token=?',(next_state,now,0 if decision=='confirmed_not_sent' else attempts,'operator_'+decision,token))
            self._history(conn,token,attempts,next_state,now,'operator_'+decision+':'+hashlib.sha256(evidence.encode()).hexdigest()[:16])
    def reap_expired_submissions(self,*,now:float)->int:
        """Hold unacknowledged provider submissions; never automatically resend."""
        now=_time(now)
        with self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            rows=conn.execute("SELECT token,attempts FROM events WHERE state='submitting' AND lease_until<=?",(now,)).fetchall()
            for token,attempt in rows:
                conn.execute("UPDATE events SET state='reconciliation_required',lease_until=NULL,last_error='expired_submission',updated_at=? WHERE token=?",(now,token))
                self._history(conn,token,attempt,'reconciliation_required',now,'expired_submission')
            return len(rows)
    def scrub_aged_unresolved(self,*,now:float,age_seconds:float=MAX_RECONCILIATION_AGE_SECONDS)->int:
        # Preserve minimal operational status and hashed audit history without PII.
        now=_time(now)
        if isinstance(age_seconds,bool) or not isinstance(age_seconds,(float,int)) or not math.isfinite(age_seconds) or not 0<age_seconds<=365*86400:
            raise ValueError('Invalid unresolved retention age')
        with self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            return conn.execute("UPDATE events SET request_json=NULL,last_error='aged_requires_operator_review' WHERE state='reconciliation_required' AND request_json IS NOT NULL AND updated_at<?",(now-age_seconds,)).rowcount
    def purge_terminal(self,*,now:float,retention_days:int=30)->int:
        now=_time(now)
        if type(retention_days) is not int or not 1<=retention_days<=365:raise ValueError('Invalid retention')
        with self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            rows=conn.execute("SELECT token,fingerprint,state FROM events WHERE state IN ('provider_accepted','permanent_failure','retry_exhausted','reconciled_cancelled') AND updated_at < ?",(now-retention_days*86400,)).fetchall()
            conn.executemany('INSERT OR REPLACE INTO event_tombstones(token_hash,fingerprint,state,archived_at) VALUES (?,?,?,?)',
                [(hashlib.sha256(token.encode()).hexdigest(),fingerprint,state,now) for token,fingerprint,state in rows])
            count=conn.execute("DELETE FROM events WHERE state IN ('provider_accepted','permanent_failure','retry_exhausted','reconciled_cancelled') AND updated_at < ?",(now-retention_days*86400,)).rowcount
            conn.execute('DELETE FROM event_tombstones WHERE archived_at<?',(now-365*86400,))
            conn.execute('DELETE FROM event_audit WHERE at<?',(now-365*86400,))
            conn.execute('DELETE FROM admission_denials WHERE window_start<?',(now-30*86400,))
        return count

def process_event(store:EventStore,*,token:str,claimed_venue:str,authenticated_venue:str,
                  recipient_key:str,preferences:Preferences,sender:Callable[[list[dict[str,str]]],str],
                  now:float,catalog_store=None,recommender:Callable|None=None,clock:Callable|None=None,
                  notify_insufficient:bool=False)->str:
    """Execute a *verified-context* event. Only HTTP handler can establish that context.

    Never automatically resubmit after a potentially accepted provider submission.
    """
    if not isinstance(authenticated_venue,str) or not authenticated_venue or claimed_venue!=authenticated_venue:
        raise PermissionError('Venue mismatch')
    recipient=normalize_recipient(recipient_key,require_email=True)
    # Direct invocation must have durable payload before claiming work; otherwise
    # a failed pre-submit attempt could enter retryable_failure forever.
    admitted=store.enqueue_request(token=token,venue=authenticated_venue,recipient=recipient,preferences=preferences,now=now)
    if admitted=='rate_limited':return 'rate_limited'
    fingerprint=fingerprint_for(authenticated_venue,recipient,preferences)
    claim=store.claim(token,fingerprint,now=now)
    if claim.status!='claimed':return claim.status
    stop_keepalive=threading.Event()
    keepalive_error=[]
    def keepalive():
        while not stop_keepalive.wait(HEARTBEAT_INTERVAL_SECONDS):
            try:store.heartbeat(token,claim.attempt,now=(clock() if clock is not None else time.time()),lease_seconds=30.0,nonce=claim.nonce)
            except Exception as err:
                keepalive_error.append(type(err).__name__)
                return
    lease_thread=threading.Thread(target=keepalive,name='palate-lease-heartbeat',daemon=True)
    lease_thread.start()
    try:
        try:
            if catalog_store is not None and not catalog_store.has_venue(authenticated_venue):
                raise CatalogDataError('Venue catalog has not been initialized')
            catalog=catalog_store.wines_for_venue(authenticated_venue) if catalog_store is not None else load_catalog()
            selection=select_candidates(catalog,authenticated_venue,preferences,max_candidates=12 if recommender is not None else 3)
            if recommender is not None:
                try:
                    output=recommender(selection,preferences)
                    validate_recommendation_contract(output,selection.wines,authenticated_venue,
                                                     preferences=preferences)
                    validate_selection_priority(output,selection)
                except (ContractError,SubmissionUncertain,KnownNotAccepted,PermanentProviderReject,OSError,TimeoutError) as error:
                    # Preserve service to the diner, but record observable degradation.
                    category=('configuration' if isinstance(error,PermanentProviderReject)
                              else 'contract' if isinstance(error,ContractError) else 'transient')
                    store.record_model_degradation(token,claim.attempt,now=(clock() if clock is not None else now),category=category,nonce=claim.nonce)
                    output=create_deterministic_recommendations(selection,preferences)
            else:
                output=create_deterministic_recommendations(selection,preferences)
            validate_recommendation_contract(output,selection.wines,authenticated_venue,
                                             preferences=preferences)
        except InsufficientCandidates:
            if notify_insufficient:
                output=[]  # Truthful no-match notification via the same durable outbox.
            else:
                store.mark_permanent_failure(token,claim.attempt,now=now,reason='insufficient_inventory',nonce=claim.nonce)
                return 'permanent_failure'
        except InputError:
            store.mark_permanent_failure(token,claim.attempt,now=now,reason='invalid_preferences',nonce=claim.nonce)
            return 'permanent_failure'
        except (CatalogDataError,ContractError):
            return store.mark_retryable_failure(token,claim.attempt,now=now,reason='catalog_or_model_failure',nonce=claim.nonce)
        except Exception:
            return store.mark_retryable_failure(token,claim.attempt,now=now,reason='pre_send_failure',nonce=claim.nonce)
        if keepalive_error:
            return 'lease_heartbeat_failed'
        # Committed BEFORE any external side effect. Once started, only manual
        # reconciliation or a proved provider non-acceptance may allow resubmission.
        current_time=clock() if clock is not None else now
        store.mark_dispatch_started(token,claim.attempt,now=current_time,nonce=claim.nonce)
        try:
            provider_ref=sender(output)
            if not isinstance(provider_ref,str) or not provider_ref.strip():raise SubmissionUncertain('No provider acceptance reference')
        except KnownNotAccepted as exc:
            try:
                return store.mark_known_not_accepted(token,claim.attempt,now=(clock() if clock is not None else now),retry_after=exc.retry_after,nonce=claim.nonce)
            except RuntimeError:
                if store.summary(token)[0]=='reconciliation_required':return 'reconciliation_required'
                raise
        except PermanentProviderReject:
            try:
                store.mark_permanent_failure(token,claim.attempt,now=(clock() if clock is not None else now),reason='provider_definitely_rejected',from_state='submitting',nonce=claim.nonce)
            except RuntimeError:
                if store.summary(token)[0]=='reconciliation_required':return 'reconciliation_required'
                raise
            return 'permanent_failure'
        except Exception:
            store.mark_uncertain(token,claim.attempt,now=(clock() if clock is not None else now),nonce=claim.nonce)
            return 'reconciliation_required'
        # If database fails here, state remains 'submitting': lease expiry holds
        # the event for reconciliation, never automatic duplicate submission.
        store.mark_provider_accepted(token,claim.attempt,provider_ref,now=(clock() if clock is not None else now),nonce=claim.nonce)
        return 'provider_accepted'
    finally:
        stop_keepalive.set()
        lease_thread.join(timeout=2.0)
