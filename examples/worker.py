"""Run the audited outbox worker explicitly; never sends on mere module import."""
from __future__ import annotations
import argparse
import json
import time
import sys
from runtime import create_worker_components,run_due

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--once',action='store_true',help='Process one bounded batch then exit')
    p.add_argument('--poll-seconds',type=float,default=10.)
    args=p.parse_args()
    if not args.once and not (1<=args.poll_seconds<=3600):p.error('Invalid polling interval')
    store,catalog,recommender,sender=create_worker_components()
    try:
        while True:
            result=run_due(store,catalog,recommender,sender)
            alerts=store.alerts(now=time.time())
            print(json.dumps({'processed_count':len(result['processed']),'held_count':result['uncertain_submissions_held'],'alerts':alerts}),flush=True)
            for alert in alerts:print('OPERATOR_ALERT '+json.dumps(alert),file=sys.stderr,flush=True)
            store.scrub_aged_unresolved(now=time.time())
            store.purge_terminal(now=time.time())
            if args.once:break
            time.sleep(args.poll_seconds)
    finally:
        store.close();catalog.close()
if __name__=='__main__':main()
