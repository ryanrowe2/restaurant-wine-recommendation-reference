"""Explicitly invoked staging-only PostgreSQL setup and legacy import."""
from __future__ import annotations
import argparse
import json
from postgres_catalog import PostgresCatalogStore,connect_from_env

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('command',choices=('init','import-legacy','bind-alias','approve-pairings'))
    parser.add_argument('--venue',help='Trusted legacy venue ID (legacy-1, etc.)')
    parser.add_argument('--wine-id',help='Stable catalog wine ID')
    parser.add_argument('--name',help='Operator-reviewed legacy wine name or corrected alias')
    parser.add_argument('--new-wine',action='store_true',help='Explicitly approve a new identity')
    parser.add_argument('--tags',nargs='+',help='Operator-reviewed source pairing tags')
    args=parser.parse_args()
    store=PostgresCatalogStore(connect_from_env())
    if args.command=='init':
        store.migrate_schema()
        print('Independent portfolio_v2 schema initialized (legacy tables unchanged)')
    elif args.command=='import-legacy':
        print(json.dumps({'migrated_counts':store.import_legacy()}))
    elif args.command=='bind-alias':
        if not all((args.venue,args.wine_id,args.name)):parser.error('Venue, wine-id and name required')
        store.bind_legacy_name(args.venue,args.name,args.wine_id,new_wine=args.new_wine)
        print('Reviewed legacy identity binding recorded')
    else:
        if not all((args.venue,args.wine_id,args.tags)):parser.error('Venue, wine-id and tags required')
        store.approve_pairings(args.venue,args.wine_id,args.tags)
        print('Reviewed pairing tags approved')
if __name__=='__main__':main()
