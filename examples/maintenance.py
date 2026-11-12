"""Operator-only recovery CLI. Requires access to the persistent private state DB.

Never expose this interface over the public webhook endpoint. Uncertain submissions can never be requeued based on text-only proof.
Use a verified provider receipt to confirm acceptance, or cancel the hold.
"""
from __future__ import annotations
import argparse
import os
import time
from delivery import EventStore
from providers import PostmarkReceiptVerifier

def main():
    p=argparse.ArgumentParser()
    sub=p.add_subparsers(dest='command',required=True)
    sub.add_parser('unresolved')
    sub.add_parser('purge')
    detail=sub.add_parser('history');detail.add_argument('token')
    action=sub.add_parser('reconcile')
    action.add_argument('token')
    action.add_argument('decision',choices=('confirmed_accepted','confirmed_not_sent','cancel'))
    action.add_argument('--evidence',required=True,help='Non-sensitive audit justification; cannot authorize replay of uncertain submissions')
    action.add_argument('--provider-ref',default=None,help='Verified provider acceptance reference')
    args=p.parse_args()
    path=os.environ.get('EVENT_DB_PATH')
    if not path or path==':memory:':p.error('Private persistent EVENT_DB_PATH required')
    store=EventStore(path)
    try:
        if args.command=='unresolved':
            for token,state,attempts,category,updated in store.unresolved():
                print(token,state,attempts,category,int(updated))
        elif args.command=='history':
            for row in store.history(args.token):print(*row)
        elif args.command=='purge':
            print('Purged terminal events:',store.purge_terminal(now=time.time()))
        else:
            verifier=None
            if args.decision=='confirmed_accepted':
                token=os.environ.get('POSTMARK_SERVER_TOKEN')
                sender=os.environ.get('SENDER_EMAIL')
                if not token or not sender:
                    p.error('POSTMARK_SERVER_TOKEN and SENDER_EMAIL required for verified acceptance')
                verifier=PostmarkReceiptVerifier(token,sender)
            store.reconcile(args.token,decision=args.decision,evidence=args.evidence,
                            provider_ref=args.provider_ref,receipt_verifier=verifier,now=time.time())
            print('Decision recorded; check provider status and local event history')
    finally:store.close()
if __name__=='__main__':main()
