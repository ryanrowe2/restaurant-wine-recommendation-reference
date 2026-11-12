"""Optional PostgreSQL persistence + transaction-safe importer for staging.

Requires psycopg2 at runtime; legacy source rows are never embedded in the
public artifact. Uses a separate schema and never mutates historical tables.
"""
from __future__ import annotations
import hashlib
import secrets
import json
import re
from pathlib import Path
from itertools import islice
from typing import Iterable
from pipeline import Wine,CatalogDataError,MAX_CATALOG_ROWS,valid_required_wine_text,valid_venue_id

MAX_ALIAS_ROWS=MAX_CATALOG_ROWS
MIGRATIONS_DIR=Path(__file__).with_name('migrations')
SCHEMA_FILE=MIGRATIONS_DIR/'001_portfolio_v2.sql'
PAIRING_TERMS={
  'red_meat':('red meat','beef','steak','lamb','burger','ribs'),
  'fish':('fish','salmon','tuna','cod'),
  'seafood':('seafood','oysters','shrimp','scallops','lobster','crab'),
  'poultry':('chicken','turkey','duck','poultry'),
  'dessert':('dessert','chocolate','cake','fruit tart'),
  'spicy_food':('spicy','thai','curry'),
  'pasta':('pasta','linguini','lasagna','risotto'),
  'vegetables':('vegetables','salad','asparagus'),
}

def extract_reviewable_pairings(source:str)->frozenset[str]:
    # Transparent lexical matches from original dish descriptions.
    # Every inferred tag must be reviewed before customer-facing use.
    text=' '.join(str(source or '').lower().split())
    text=re.sub(r'\b(?:doesn.t|don.t|isn.t|aren.t|wasn.t|weren.t|won.t|can.t)\b', 'not', text)
    if re.search(r'\b(?:avoid|without|not(?:\s+(?:good|great|ideal|recommended|suitable|paired|pair|with|for|to|go))?|does\s+not\s+(?:go|pair)|do\s+not\s+pair|never\s+pair|no\s+pairing|poor\s+(?:with|for))\b',text):
        return frozenset()  # Preserve ambiguous/negated prose for human review, never infer tags.
    return frozenset(tag for tag,phrases in PAIRING_TERMS.items() if any(re.search(r'(?<![a-z])'+re.escape(phrase)+r'(?![a-z])',text) for phrase in phrases))

def restored_pronunciation(*candidates:str)->str:
    # Never reactivate quarantined/unreviewed archival pronunciation.
    placeholders={'Pronunciation not provided','Pronunciation requires review'}
    for candidate in candidates:
        if valid_required_wine_text(candidate) and candidate not in placeholders:
            return candidate
    return 'Pronunciation not provided'

def stable_legacy_wine_id(restaurant_id:str,wine_name:str)->str:
    if not wine_name.strip():raise CatalogDataError('Legacy wine missing name')
    norm=' '.join(wine_name.lower().split())
    return hashlib.sha256(f'{restaurant_id}:{norm}'.encode()).hexdigest()[:24]

def legacy_row_to_wine(row:tuple)->Wine:
    """Read a snapshot of the historical legacy public.wines table.

    Columns: restaurant_id,wine_name,by_glass,by_bottle,origin,category,
    type,characteristics,body,sweet_or_dry,general_pairings,metadata.
    Original metadata and pairing descriptions are not falsely treated as
    structured tags; the raw fields remain in the old table and metadata field.
    """
    if len(row)!=12:raise CatalogDataError('Legacy row shape mismatch')
    venue,name,glass,bottle,origin,category,wine_type,description,body,sweetness,pairings,metadata=row
    if type(venue) is not int or venue<0:
        raise CatalogDataError('Invalid legacy restaurant identity')
    venue_id=f'legacy-{venue}'
    # Never coerce NULL or another SQL type into a plausible wine name.
    if (not isinstance(name,str) or not name.strip()
        or len(name)>1500):
        raise CatalogDataError('Legacy wine missing or invalid name')
    if type(glass) is not bool or type(bottle) is not bool:raise CatalogDataError('Availability requires Boolean values')
    # Historical metadata frequently contains unverified pairing tags (even for
    # unrelated wines). Keep the verbatim value in source_metadata; never
    # silently activate unreviewed tags as recommendation characteristics.
    tags=frozenset()
    return Wine(venue_id,stable_legacy_wine_id(venue_id,name),name,
                'Pronunciation not provided',str(origin or 'Origin not provided'),
                str(category),str(wine_type or 'Type not provided'),str(sweetness),
                tags,glass,bottle,frozenset())  # Inferred pairings are unapproved; stage separately

# Validate the deployed shape, not just receipts and column names. A migration
# hash proves what our files contain, not whether an operator changed the DB.
REQUIRED_SCHEMA_COLUMNS={
 'venues':{'venue_id':'text','display_name':'text'},
 'wines':{
   'venue_id':'text','wine_id':'text','wine_name':'text','pronunciation':'text',
   'origin':'text','category':'text','wine_type':'text','sweetness':'text',
   'characteristics':'jsonb','pairings':'jsonb','available_by_glass':'bool',
   'available_by_bottle':'bool','source_metadata':'text',
   'source_characteristics':'text','source_body':'text','source_pairings':'text',
   'suggested_pairings':'jsonb','pairings_reviewed':'bool'},
 'legacy_identity_map':{
   'venue_id':'text','legacy_name':'text','wine_id':'text','reviewed':'bool'},
 'wine_review_archive':{
   'venue_id':'text','wine_id':'text','pronunciation':'text',
   'characteristics':'jsonb','pairings':'jsonb','suggested_pairings':'jsonb',
   'pairings_reviewed':'bool','source_metadata':'text','source_characteristics':'text',
   'source_body':'text','source_pairings':'text'},
 'schema_migrations':{'version':'text','sha256':'text','applied_at':'timestamptz'},
 'archive_text_quarantine_v16':{
    'venue_id':'text','wine_id':'text','original_pronunciation':'text','reason':'text'},
}
NULLABLE_WINE_COLUMNS=frozenset(('source_metadata','source_characteristics','source_body','source_pairings'))

def _normalized_check_expression(definition:str)->str:
    """Conservative normalization of expected pg_get_constraintdef CHECK SQL.

    This is intentionally an allowlist, not a search for reassuring keywords.
    The live schema drill below checks actual enforcement in addition.
    """
    raw=re.sub(r'\s+','',definition.lower().replace('"','').replace('::text','').replace('portfolio_v2.',''))
    if not raw.startswith('check(') or not raw.endswith(')'):
        return ''
    expression=raw[6:-1]
    while expression.startswith('(') and expression.endswith(')'):
        depth=0;full=True
        for index,char in enumerate(expression):
            if char=='(':depth+=1
            elif char==')':depth-=1
            if depth==0 and index!=len(expression)-1:
                full=False;break
        if not full:break
        expression=expression[1:-1]
    return expression


def _validate_schema_metadata(columns, constraints, indexes)->None:
    """Reject schema drift using *actual* pg_constraint key arrays and types.

    Every constraint row must contain ordered local and referenced column names
    obtained from conkey/confkey; textual mentions of venue_id are insufficient.
    """
    actual={(table,col):(typ,nullable) for table,col,typ,nullable in columns}
    for table,spec in REQUIRED_SCHEMA_COLUMNS.items():
        for col,kind in spec.items():
            want_nullable='YES' if table in ('wines','wine_review_archive') and col in NULLABLE_WINE_COLUMNS else 'NO'
            if actual.get((table,col))!=(kind,want_nullable):
                raise RuntimeError(f'Schema drift: {table}.{col} type/nullability differs')
    if any(len(row)!=8 for row in constraints):
        raise RuntimeError('Schema drift: missing constraint validation metadata or ordered key columns')
    definitions={}
    for table,name,kind,definition,validated,local,referenced_table,referenced_columns in constraints:
        if validated is not True or 'not enforced' in definition.lower():
            raise RuntimeError(f'Schema drift: unvalidated or unenforced constraint: {table}.{name}')
        if not isinstance(local,(tuple,list)) or not isinstance(referenced_columns,(tuple,list)):
            raise RuntimeError('Schema drift: invalid constraint key metadata')
        key=(table,name)
        if key in definitions:
            raise RuntimeError('Schema drift: duplicated constraint metadata')
        definitions[key]=(kind,definition,tuple(local),referenced_table,tuple(referenced_columns))
    expected_keys={
        'venues':('venue_id',), 'wines':('venue_id','wine_id'),
        'legacy_identity_map':('venue_id','legacy_name'),
        'wine_review_archive':('venue_id','wine_id'),
        'schema_migrations':('version',),
        'archive_text_quarantine_v16':('venue_id','wine_id'),
    }
    for table,key_columns in expected_keys.items():
        row=definitions.get((table,table+'_pkey'))
        if row is None or row[0]!='p' or row[2]!=key_columns:
            raise RuntimeError(f'Schema drift: {table} primary key columns altered')
    for table in ('wines','legacy_identity_map','wine_review_archive'):
        fks=[item for (t,_),item in definitions.items() if t==table and item[0]=='f']
        if not any(item[2]==('venue_id',) and item[3]=='venues' and item[4]==('venue_id',)
                   for item in fks):
            raise RuntimeError(f'Schema drift: {table} venue foreign key columns altered')
    # Historical evidence must not survive without its normalized key columns.
    # This is intentionally an exact shape check, not a migration receipt check.
    quarantine_cols={col for (table,col),_ in actual.items()
                     if table=='archive_text_quarantine_v16'}
    if quarantine_cols!=set(REQUIRED_SCHEMA_COLUMNS['archive_text_quarantine_v16']):
        raise RuntimeError('Schema drift: quarantine evidence table fields changed')
    wine_checks=[_normalized_check_expression(row[1]) for (table,_),row in definitions.items()
                 if table=='wines' and row[0]=='c']
    archive_checks=[_normalized_check_expression(row[1]) for (table,_),row in definitions.items()
                    if table=='wine_review_archive' and row[0]=='c']
    def allowed_enum(column,values):
        items=','.join("'"+value+"'" for value in values)
        return (f'{column}in({items})',f'{column}=any(array[{items}])')
    for column,values in (
        ('category',('red','white','sparkling','rose')),
        ('sweetness',('dry','semi-dry','semi-sweet','sweet')),
    ):
        if not any(check in allowed_enum(column,values) for check in wine_checks):
            raise RuntimeError('Schema drift: ineffective wine check constraint on '+column)
    if 'available_by_glassoravailable_by_bottle' not in wine_checks:
        raise RuntimeError('Schema drift: availability check constraint weakened')
    for column in ('characteristics','pairings','suggested_pairings'):
        if f'valid_tag_array({column})' not in wine_checks:
            raise RuntimeError('Schema drift: required wine check constraint missing: '+column)
    archive_required='and'.join(f'valid_tag_array({col})' for col in
                              ('characteristics','pairings','suggested_pairings'))
    if archive_required not in archive_checks:
        raise RuntimeError('Schema drift: archived review check constraint weakened')
    required_fields=('wine_id','wine_name','pronunciation','origin','wine_type')
    def proper_required_text(check):
        if not check or 'ortrue' in check or ')or(' in check or ')ortrue' in check:
            return False
        return all((f'length(btrim({field}))between1and1500' in check or
                    (f'length(btrim({field}))>=1' in check and
                     f'length(btrim({field}))<=1500' in check)) and
                   f'length({field})<=1500' in check for field in required_fields)
    if not any(proper_required_text(check) for check in wine_checks):
        raise RuntimeError('Schema drift: required wine text fields check weakened')
    v15_contract=definitions.get(('wines','wine_required_text_contract_v15'))
    v15_check=_normalized_check_expression(v15_contract[1]) if v15_contract and v15_contract[0]=='c' else ''
    if not v15_check or any(f'valid_required_wine_text({field})' not in v15_check for field in required_fields):
        raise RuntimeError('Schema drift: v15 required-text contract missing or weakened')
    for table,fields in (('wines',required_fields),('wine_review_archive',('pronunciation',))):
        guarded=definitions.get((table,table+'_visible_text_contract_v16'))
        expression=_normalized_check_expression(guarded[1]) if guarded and guarded[0]=='c' else ''
        if not expression or not all(f'valid_visible_wine_text({field})' in expression for field in fields):
            raise RuntimeError('Schema drift: v16 display-safe text contract missing: '+table)
    # Additional CHECKs can narrow the permitted domain while leaving every
    # required CHECK present. Reject unrecognized wine CHECK definitions: the
    # explicitly supported catalog contract must not silently become narrower.
    known_checks=set()
    for column,values in (
        ('category',('red','white','sparkling','rose')),
        ('sweetness',('dry','semi-dry','semi-sweet','sweet')),
    ):
        known_checks.update(allowed_enum(column,values))
    known_checks.add('available_by_glassoravailable_by_bottle')
    known_checks.update(f'valid_tag_array({column})' for column in
                        ('characteristics','pairings','suggested_pairings'))
    known_checks.update(f"jsonb_typeof({column})='array'" for column in
                        ('characteristics','pairings'))
    known_checks.update(f'length({column})>0' for column in
                        ('wine_id','wine_name','pronunciation','origin'))
    for check in wine_checks:
        if check not in known_checks and check!=v15_check and not ('valid_visible_wine_text(' in check) and not proper_required_text(check):
            raise RuntimeError('Schema drift: unexpected or restrictive wine CHECK constraint: '+check[:150])
    idx=[definition.lower().replace('"','').strip() for table,name,definition in indexes
         if table=='wines' and name=='wines_venue_category']
    if len(idx)!=1 or not re.fullmatch(
       r'create\s+index\s+wines_venue_category\s+on\s+(?:portfolio_v2\.)?wines'
       r'(?:\s+using\s+btree)?\s*\(\s*venue_id\s*,\s*category\s*,\s*sweetness\s*\)',idx[0]):
        raise RuntimeError('Schema drift: venue/category/sweetness index missing, partial or altered')


def _assert_database_checks_enforced(cur)->None:
    """Transaction-scoped negative INSERT controls, rolled back unconditionally.

    Schema names and SQL definitions alone cannot prove CHECK enforcement.
    A genuine check failure must report SQLSTATE 23514; other SQL failures
    do not count as evidence of the desired constraint.
    """
    # These checks run after metadata verification and within migrate_schema's
    # transaction. Nested savepoints remove all synthetic rows on either path.
    cur.execute('SAVEPOINT palate_schema_audit')
    try:
        venue='__palate_integrity_probe__'+secrets.token_hex(8)
        cur.execute('INSERT INTO portfolio_v2.venues(venue_id,display_name) VALUES (%s,%s) ON CONFLICT (venue_id) DO NOTHING',
                    (venue,'Synthetic schema probe'))
        base=dict(venue_id=venue,wine_id='audit-synthetic',wine_name='Synthetic Wine',
                  pronunciation='Synthetic',origin='Imaginary',category='red',wine_type='merlot',
                  sweetness='dry',characteristics='[]',pairings='[]',suggested_pairings='[]',
                  available_by_glass=True,available_by_bottle=False)
        invalid=(
          {'category':'unsafe_category'}, {'sweetness':'unsafe_sweetness'},
          {'available_by_glass':False,'available_by_bottle':False},
          {'characteristics':'[12]'}, {'pairings':'[12]'},
          {'suggested_pairings':'[12]'},
          {'characteristics':'["bold","bold"]'},
          {'pairings':'["z","a"]'},
          {'suggested_pairings':'["z","a"]'}, {'wine_name':'   '},
          {'wine_type':' ' * 1501},
          {'wine_name':'\t'}, {'wine_name':'\n'}, {'wine_name':'\x1f'},
          {'wine_name':'\x7f'}, {'wine_name':'\u00a0'},
          {'wine_type':'Type\x1f'},
        )
        insert="""INSERT INTO portfolio_v2.wines
          (venue_id,wine_id,wine_name,pronunciation,origin,category,wine_type,sweetness,
           characteristics,pairings,suggested_pairings,available_by_glass,available_by_bottle)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s,%s)"""
        keys=tuple(base)
        for number,mutation in enumerate(invalid):
            cur.execute('SAVEPOINT palate_schema_case')
            try:
                values={**base,**mutation,'wine_id':f'audit-synthetic-{number}'}
                try:
                    cur.execute(insert,tuple(values[key] for key in keys))
                except Exception as error:
                    if getattr(error,'pgcode',None)!='23514':
                        raise RuntimeError('Schema drill failed for another reason, not the target CHECK') from error
                else:
                    raise RuntimeError('Schema drift: ineffective persisted CHECK constraint: '+repr(mutation))
            finally:
                cur.execute('ROLLBACK TO SAVEPOINT palate_schema_case')
                cur.execute('RELEASE SAVEPOINT palate_schema_case')
    finally:
        cur.execute('ROLLBACK TO SAVEPOINT palate_schema_audit')
        cur.execute('RELEASE SAVEPOINT palate_schema_audit')

def _assert_required_text_updates_enforced(cur)->None:
    """Verify SQL blocks invalid *updates* as well as inserts, under savepoints.

    Called from migration validation only; the valid probe row is rolled back.
    """
    cur.execute('SAVEPOINT palate_required_update_audit')
    try:
        venue='__palate_update_probe__'+secrets.token_hex(8)
        cur.execute('INSERT INTO portfolio_v2.venues(venue_id,display_name) VALUES (%s,%s)',
                    (venue,'Synthetic update probe'))
        cur.execute("""INSERT INTO portfolio_v2.wines
          (venue_id,wine_id,wine_name,pronunciation,origin,category,wine_type,sweetness,
           characteristics,pairings,suggested_pairings,available_by_glass,available_by_bottle)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s,%s)""",
          (venue,'audit-update','Original valid name','Fictional','Imaginary',
           'red','Fictional type','dry','[]','[]','[]',True,False))
        for field,value in (('wine_name','\t'),('wine_name','\n'),
                            ('wine_name','\u00a0'),('origin','\x1f'),
                            ('wine_type','Fake\x7f')):
            # Column identifiers are fixed literals above, not user input.
            cur.execute('SAVEPOINT palate_required_update_case')
            try:
                try:
                    cur.execute(f'UPDATE portfolio_v2.wines SET {field}=%s WHERE venue_id=%s AND wine_id=%s',
                                (value,venue,'audit-update'))
                except Exception as exc:
                    if getattr(exc,'pgcode',None)!='23514':
                        raise RuntimeError('Required-text UPDATE drill failed for another reason') from exc
                else:
                    raise RuntimeError('Schema drift: invalid required-text UPDATE accepted')
            finally:
                cur.execute('ROLLBACK TO SAVEPOINT palate_required_update_case')
                cur.execute('RELEASE SAVEPOINT palate_required_update_case')
    finally:
        cur.execute('ROLLBACK TO SAVEPOINT palate_required_update_audit')
        cur.execute('RELEASE SAVEPOINT palate_required_update_audit')

def _assert_database_positive_controls(cur)->None:
    """Rollback-only positive INSERT and UPDATE probes for supported inventory.

    A schema may still contain extra restrictive constraints/triggers even if
    all mandatory constraints work. Verify the legitimate domain survives.
    Full certification requires execution against a real PostgreSQL database.
    """
    cur.execute('SAVEPOINT palate_positive_audit')
    try:
        venue='__palate_positive_probe__'+secrets.token_hex(8)
        try:
            cur.execute('INSERT INTO portfolio_v2.venues(venue_id,display_name) VALUES (%s,%s)',
                        (venue,'Fictional positive schema probe'))
        except Exception as exc:
            raise RuntimeError('Schema drift: valid venue INSERT rejected') from exc
        insert="""INSERT INTO portfolio_v2.wines
          (venue_id,wine_id,wine_name,pronunciation,origin,category,wine_type,sweetness,
           characteristics,pairings,suggested_pairings,available_by_glass,available_by_bottle)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s,%s)"""
        update="""UPDATE portfolio_v2.wines SET category=%s,sweetness=%s,
          available_by_glass=%s,available_by_bottle=%s WHERE venue_id=%s AND wine_id=%s"""
        for i,category in enumerate(('red','white','sparkling','rose')):
            for j,sweetness in enumerate(('dry','semi-dry','semi-sweet','sweet')):
                wine_id=f'audit-positive-{i}-{j}'
                try:
                    cur.execute(insert,(venue,wine_id,'Fictional Wine','Fictional',
                       'Imaginary',category,'Fictional type',sweetness,'["bold"]','[]','[]',True,False))
                    # Assert INSERT persisted the values, not just that it returned.
                    cur.execute("""SELECT wine_name,pronunciation,origin,category,wine_type,sweetness,
                        characteristics,pairings,suggested_pairings,available_by_glass,available_by_bottle
                        FROM portfolio_v2.wines WHERE venue_id=%s AND wine_id=%s""",
                        (venue,wine_id))
                    expected=('Fictional Wine','Fictional','Imaginary',category,
                              'Fictional type',sweetness,['bold'],[],[],True,False)
                    observed=cur.fetchone()
                    if observed is None or tuple(observed)!=expected:
                        raise RuntimeError('Schema drift: positive INSERT data silently rewritten')
                    # UPDATEs of the same supported dimensions must also work.
                    cur.execute(update,(category,sweetness,False,True,venue,wine_id))
                    cur.execute("""SELECT wine_name,pronunciation,origin,category,wine_type,sweetness,
                        characteristics,pairings,suggested_pairings,available_by_glass,available_by_bottle
                        FROM portfolio_v2.wines WHERE venue_id=%s AND wine_id=%s""",
                        (venue,wine_id))
                    after=cur.fetchone()
                    if after is None or tuple(after)!=(*expected[:-2],False,True):
                        raise RuntimeError('Schema drift: positive UPDATE data silently rewritten')
                except Exception as exc:
                    if isinstance(exc,RuntimeError) and 'silently rewritten' in str(exc):
                        raise
                    raise RuntimeError('Schema drift: supported wine category/sweetness INSERT/UPDATE rejected: '
                                       +category+'/'+sweetness) from exc
    finally:
        cur.execute('ROLLBACK TO SAVEPOINT palate_positive_audit')
        cur.execute('RELEASE SAVEPOINT palate_positive_audit')

def _assert_quarantine_table_control(cur)->None:
    """Read/write control for quarantine evidence; all synthetic rows rollback."""
    cur.execute('SAVEPOINT palate_quarantine_audit')
    try:
        venue='__palate_quarantine__'+secrets.token_hex(8)
        wine='audit-synthetic'
        original='\t\u200b hidden pronunciation'
        reason='invalid_legacy_display_text'
        cur.execute('''INSERT INTO portfolio_v2.archive_text_quarantine_v16
            (venue_id,wine_id,original_pronunciation,reason)
            VALUES (%s,%s,%s,%s)''',(venue,wine,original,reason))
        cur.execute('''SELECT original_pronunciation,reason FROM portfolio_v2.archive_text_quarantine_v16
            WHERE venue_id=%s AND wine_id=%s''',(venue,wine))
        if cur.fetchone()!=(original,reason):
            raise RuntimeError('Schema drift: quarantine evidence does not survive readback')
    finally:
        cur.execute('ROLLBACK TO SAVEPOINT palate_quarantine_audit')
        cur.execute('RELEASE SAVEPOINT palate_quarantine_audit')

class PostgresCatalogStore:
    def __init__(self,connection_factory):
        self.connection_factory=connection_factory
    @staticmethod
    def _legacy_alias(name:str)->str:
        if not isinstance(name,str) or not name.strip():
            raise CatalogDataError('Legacy alias requires a nonempty name')
        return ' '.join(name.lower().split())
    def bind_legacy_name(self,venue_id:str,new_name:str,wine_id:str,*,new_wine:bool=False)->None:
        """Operator-reviewed alias/rename registration for future legacy imports.

        new_wine=True explicitly authorizes a newly observed wine, while False
        requires wine_id to exist already (a rename or corrected spelling).
        """
        alias=self._legacy_alias(new_name)
        if not valid_venue_id(venue_id) or not valid_required_wine_text(wine_id):
            raise ValueError('Invalid reviewed identity')
        conn=self.connection_factory()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT 1 FROM portfolio_v2.wines WHERE venue_id=%s AND wine_id=%s',(venue_id,wine_id))
                    present=cur.fetchone() is not None
                    if not present and not new_wine:
                        raise ValueError('Existing stable wine ID required for rename')
                    cur.execute("""INSERT INTO portfolio_v2.legacy_identity_map(venue_id,legacy_name,wine_id,reviewed)
                                   VALUES (%s,%s,%s,TRUE)
                                   ON CONFLICT (venue_id,legacy_name) DO UPDATE SET reviewed=TRUE
                                   WHERE portfolio_v2.legacy_identity_map.wine_id=EXCLUDED.wine_id""",(venue_id,alias,wine_id))
                    cur.execute('SELECT wine_id FROM portfolio_v2.legacy_identity_map WHERE venue_id=%s AND legacy_name=%s',(venue_id,alias))
                    if cur.fetchone()[0]!=wine_id:raise ValueError('Alias conflicts with existing identity')
        finally:conn.close()
    @staticmethod
    def _archive_active(cur,venue_id:str)->None:
        """Persist operator-owned review and provenance independently of stock.

        Must run in the same transaction and after the venue row is locked.
        Identity is (venue_id,wine_id); unrelated venues cannot be affected.
        """
        cur.execute("""INSERT INTO portfolio_v2.wine_review_archive(
             venue_id,wine_id,pronunciation,characteristics,pairings,
             suggested_pairings,pairings_reviewed,source_metadata,
             source_characteristics,source_body,source_pairings)
           SELECT venue_id,wine_id,pronunciation,characteristics,pairings,
             suggested_pairings,pairings_reviewed,source_metadata,
             source_characteristics,source_body,source_pairings
           FROM portfolio_v2.wines WHERE venue_id=%s
           ON CONFLICT(venue_id,wine_id) DO UPDATE SET
             pronunciation=EXCLUDED.pronunciation,
             characteristics=EXCLUDED.characteristics,
             pairings=EXCLUDED.pairings,
             suggested_pairings=EXCLUDED.suggested_pairings,
             pairings_reviewed=EXCLUDED.pairings_reviewed,
             source_metadata=EXCLUDED.source_metadata,
             source_characteristics=EXCLUDED.source_characteristics,
             source_body=EXCLUDED.source_body,
             source_pairings=EXCLUDED.source_pairings""",(venue_id,))

    @staticmethod
    def _read_archive(cur,venue_id:str)->dict:
        cur.execute("""SELECT wine_id,pronunciation,characteristics,pairings,
            pairings_reviewed,source_metadata,source_characteristics,source_body,
            source_pairings,suggested_pairings
            FROM portfolio_v2.wine_review_archive WHERE venue_id=%s
            ORDER BY wine_id LIMIT %s""",(venue_id,MAX_CATALOG_ROWS+1))
        rows=cur.fetchmany(MAX_CATALOG_ROWS+1)
        if len(rows)>MAX_CATALOG_ROWS:
            raise CatalogDataError('Persisted review archive exceeds row limit')
        return {row[0]:row[1:] for row in rows}

    @staticmethod
    def _json_tags(raw)->list:
        return raw if isinstance(raw,list) else json.loads(raw)

    def approve_pairings(self,venue_id:str,wine_id:str,tags:Iterable[str])->None:
        """Promote operator-reviewed inferred tags to recommendation-visible tags."""
        proposed=frozenset(tags)
        if not proposed or any(not isinstance(t,str) for t in proposed):raise ValueError('Approved tags required')
        conn=self.connection_factory()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT suggested_pairings FROM portfolio_v2.wines WHERE venue_id=%s AND wine_id=%s FOR UPDATE',(venue_id,wine_id))
                    row=cur.fetchone()
                    if not row:raise ValueError('Unknown wine identity')
                    available=set(row[0] if isinstance(row[0],list) else json.loads(row[0]))
                    if not proposed.issubset(available):raise ValueError('Tags require review against original source')
                    cur.execute("""UPDATE portfolio_v2.wines SET pairings=%s::jsonb,pairings_reviewed=TRUE
                                  WHERE venue_id=%s AND wine_id=%s""",(json.dumps(sorted(proposed)),venue_id,wine_id))
                    self._archive_active(cur,venue_id)
        finally:conn.close()
    def close(self):
        """Connections are per-operation; nothing persistent to close."""
        return None
    def has_venue(self,venue_id:str)->bool:
        conn=self.connection_factory()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM portfolio_v2.venues WHERE venue_id=%s LIMIT 1",(venue_id,))
                return cur.fetchone() is not None
        finally:conn.close()
    def has_minimum_inventory(self,venue_id:str,minimum:int=3)->bool:
        if type(minimum) is not int or minimum<1 or minimum>MAX_CATALOG_ROWS:
            raise ValueError('Invalid inventory threshold')
        conn=self.connection_factory()
        try:
            with conn.cursor() as cur:
                cur.execute('SELECT 1 FROM portfolio_v2.wines WHERE venue_id=%s LIMIT %s',(venue_id,minimum))
                return len(cur.fetchall())==minimum
        finally:conn.close()
    def migrate_schema(self):
        # Each migration has an immutable SHA256 receipt and runs transactionally.
        conn=self.connection_factory()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute('CREATE SCHEMA IF NOT EXISTS portfolio_v2')
                    cur.execute('''CREATE TABLE IF NOT EXISTS portfolio_v2.schema_migrations
                      (version TEXT PRIMARY KEY,sha256 TEXT NOT NULL CHECK(length(sha256)=64),applied_at TIMESTAMPTZ NOT NULL DEFAULT now())''')
                    cur.execute('SELECT pg_advisory_xact_lock(7782341)')
                    for path in sorted(MIGRATIONS_DIR.glob('[0-9][0-9][0-9]_*.sql')):
                        content=path.read_text(encoding='utf-8')
                        digest=hashlib.sha256(content.encode()).hexdigest()
                        cur.execute('SELECT sha256 FROM portfolio_v2.schema_migrations WHERE version=%s',(path.name,))
                        previous=cur.fetchone()
                        if previous and previous[0]!=digest:raise RuntimeError('Schema drift detected for '+path.name)
                        if not previous:
                            cur.execute(content)
                            cur.execute('INSERT INTO portfolio_v2.schema_migrations(version,sha256) VALUES(%s,%s)',(path.name,digest))
                    cur.execute("""SELECT table_name,column_name,udt_name,is_nullable
                      FROM information_schema.columns WHERE table_schema='portfolio_v2'""")
                    columns=cur.fetchall()
                    cur.execute("""SELECT cls.relname,con.conname,con.contype,
                        pg_get_constraintdef(con.oid,TRUE),con.convalidated,
                        ARRAY(SELECT a.attname FROM unnest(con.conkey) WITH ORDINALITY AS k(num,ord)
                              JOIN pg_attribute a ON a.attrelid=con.conrelid AND a.attnum=k.num
                              ORDER BY k.ord),
                        referenced.relname,
                        ARRAY(SELECT a.attname FROM unnest(con.confkey) WITH ORDINALITY AS k(num,ord)
                              JOIN pg_attribute a ON a.attrelid=con.confrelid AND a.attnum=k.num
                              ORDER BY k.ord)
                      FROM pg_constraint con JOIN pg_class cls ON cls.oid=con.conrelid
                      JOIN pg_namespace ns ON ns.oid=cls.relnamespace
                      LEFT JOIN pg_class referenced ON referenced.oid=con.confrelid
                      WHERE ns.nspname='portfolio_v2'""")
                    constraints=cur.fetchall()
                    cur.execute("""SELECT tablename,indexname,indexdef FROM pg_indexes
                      WHERE schemaname='portfolio_v2'""")
                    indexes=cur.fetchall()
                    _validate_schema_metadata(columns,constraints,indexes)
                    # pg_indexes does not expose whether an index is valid,
                    # ready, live, or backed by the expected access method.
                    cur.execute("""SELECT ix.indisvalid,ix.indisready,ix.indislive,
                        (ix.indpred IS NULL),am.amname
                        FROM pg_index ix JOIN pg_class idx ON idx.oid=ix.indexrelid
                        JOIN pg_class tbl ON tbl.oid=ix.indrelid
                        JOIN pg_namespace ns ON ns.oid=tbl.relnamespace
                        JOIN pg_am am ON am.oid=idx.relam
                        WHERE ns.nspname='portfolio_v2' AND tbl.relname='wines'
                          AND idx.relname='wines_venue_category'""")
                    if cur.fetchall()!=[(True,True,True,True,'btree')]:
                        raise RuntimeError('Schema drift: venue/category/sweetness index unusable')
                    # Exercise the function, since an administrator could have
                    # replaced its body without changing the migration receipts.
                    cur.execute('''SELECT portfolio_v2.valid_tag_array('["bold"]'::jsonb),
                      portfolio_v2.valid_tag_array('"bold"'::jsonb),
                      portfolio_v2.valid_tag_array('[12]'::jsonb),
                      portfolio_v2.valid_tag_array('[" "]'::jsonb),
                      portfolio_v2.valid_tag_array('["a b"]'::jsonb),
                      portfolio_v2.valid_tag_array('["red_meat"]'::jsonb),
                      portfolio_v2.valid_tag_array('["bold","bold"]'::jsonb),
                      portfolio_v2.valid_tag_array('["z","a"]'::jsonb),
                      portfolio_v2.valid_tag_array('["a","z"]'::jsonb)''')
                    if cur.fetchone()!=(True,False,False,False,False,True,False,False,True):
                        raise RuntimeError('Schema drift: tag array validation function differs')
                    # Unexpected user triggers/rules may rewrite accepted values.
                    # System triggers backing PostgreSQL FK constraints are allowed.
                    cur.execute("""SELECT t.tgname FROM pg_trigger t
                       JOIN pg_class rel ON rel.oid=t.tgrelid
                       JOIN pg_namespace ns ON ns.oid=rel.relnamespace
                       WHERE ns.nspname='portfolio_v2' AND rel.relname IN ('wines','wine_review_archive')
                         AND NOT t.tgisinternal AND t.tgenabled <> 'D'""")
                    if cur.fetchall():
                        raise RuntimeError('Schema drift: unreviewed application triggers')
                    cur.execute("""SELECT r.rulename FROM pg_rewrite r
                       JOIN pg_class rel ON rel.oid=r.ev_class
                       JOIN pg_namespace ns ON ns.oid=rel.relnamespace
                       WHERE ns.nspname='portfolio_v2' AND rel.relname IN ('wines','wine_review_archive')
                         AND r.rulename <> '_RETURN'""")
                    if cur.fetchall():
                        raise RuntimeError('Schema drift: unreviewed table rewrite rules')
                    _assert_database_checks_enforced(cur)
                    # Function body is deployed from the pinned additive migration;
                    # live negative drills below verify it is still enforcing.
                    _assert_required_text_updates_enforced(cur)
                    _assert_database_positive_controls(cur)
                    _assert_quarantine_table_control(cur)
        finally:conn.close()
    def replace_venue(self,venue_id:str,wines:Iterable[Wine],*,name:str='Fictional venue',allow_empty:bool=False)->int:
        records=tuple(islice(wines,MAX_CATALOG_ROWS+1))
        if (not valid_venue_id(venue_id)
            or type(allow_empty) is not bool or (not records and not allow_empty)
            or len(records)>MAX_CATALOG_ROWS
            or any(not isinstance(w,Wine) or w.restaurant_id!=venue_id for w in records)
            or len({w.wine_id for w in records})!=len(records)):
            raise ValueError('Invalid venue replacement')
        conn=self.connection_factory()
        try:
            with conn:
                with conn.cursor() as cur:
                    if records:
                        cur.execute('INSERT INTO portfolio_v2.venues(venue_id,display_name) VALUES (%s,%s) ON CONFLICT(venue_id) DO NOTHING',(venue_id,name))
                    cur.execute('SELECT venue_id FROM portfolio_v2.venues WHERE venue_id=%s FOR UPDATE',(venue_id,))
                    if not records and cur.fetchone() is None:
                        raise ValueError('Empty replacement requires an existing registered venue')
                    cur.execute("""SELECT wine_id,source_metadata,source_characteristics,source_body,source_pairings,
                                   suggested_pairings,pairings_reviewed,pairings
                                   FROM portfolio_v2.wines WHERE venue_id=%s ORDER BY wine_id LIMIT %s""",(venue_id,MAX_CATALOG_ROWS+1))
                    previous_rows=cur.fetchmany(MAX_CATALOG_ROWS+1)
                    if len(previous_rows)>MAX_CATALOG_ROWS:
                        raise CatalogDataError('Persisted venue inventory exceeds administrative row limit')
                    previous={r[0]:r[1:] for r in previous_rows}
                    self._archive_active(cur,venue_id)
                    archived=self._read_archive(cur,venue_id)
                    cur.execute('DELETE FROM portfolio_v2.wines WHERE venue_id=%s',(venue_id,))
                    for w in records:
                        prior=previous.get(w.wine_id)
                        saved=archived.get(w.wine_id)
                        source=(prior[:4] if prior else saved[4:8] if saved else (None,None,None,None))
                        suggested=(prior[4] if prior else saved[8] if saved else [])
                        reviewed=(prior[5] if prior else saved[3] if saved else True)
                        approved=(prior[6] if prior else saved[2] if saved else None)
                        if prior or saved:
                            # Reviewed enrichment and the review flag are durable
                            # operator-owned state. Catalog replacement cannot erase
                            # approved tags or implicitly approve suggestions.
                            active_pairings=(self._json_tags(approved) if reviewed and approved is not None else [])
                        else:
                            active_pairings=sorted(w.pairings)
                        # Preserve operator-reviewed traits across temporary
                        # inventory disappearance. Fresh source traits may add
                        # descriptors; removals require an explicit review edit.
                        active_characteristics=(sorted(set(w.characteristics)|
                            set(self._json_tags(saved[1]))) if saved else sorted(w.characteristics))
                        cur.execute("""INSERT INTO portfolio_v2.wines(venue_id,wine_id,wine_name,pronunciation,origin,category,wine_type,sweetness,
                         characteristics,pairings,available_by_glass,available_by_bottle,source_metadata,source_characteristics,source_body,source_pairings,
                         suggested_pairings,pairings_reviewed)
                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)""",
                         (venue_id,w.wine_id,w.wine_name,restored_pronunciation(saved[0] if saved else None,w.pronunciation),w.origin,w.category,w.type,w.sweetness,
                          json.dumps(active_characteristics),json.dumps(active_pairings),w.available_by_glass,w.available_by_bottle,
                          *source,json.dumps(suggested if isinstance(suggested,list) else json.loads(suggested)),reviewed))
            return len(records)
        finally:conn.close()
    def wines_for_venue(self,venue_id:str)->tuple[Wine,...]:
        conn=self.connection_factory()
        try:
            with conn.cursor() as cur:
                cur.execute('''SELECT venue_id,wine_id,wine_name,pronunciation,origin,category,wine_type,sweetness,
                 characteristics,pairings,available_by_glass,available_by_bottle FROM portfolio_v2.wines WHERE venue_id=%s ORDER BY wine_id LIMIT %s''',(venue_id,MAX_CATALOG_ROWS+1))
                rows=cur.fetchmany(MAX_CATALOG_ROWS+1)
                if len(rows)>MAX_CATALOG_ROWS:
                    raise CatalogDataError('Persisted PostgreSQL catalog exceeds row limit')
            try:
                return tuple(Wine(r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],frozenset(r[8] if isinstance(r[8],list) else json.loads(r[8])),r[10],r[11],frozenset(r[9] if isinstance(r[9],list) else json.loads(r[9]))) for r in rows)
            except (ValueError,TypeError,CatalogDataError) as error:
                raise CatalogDataError('Corrupt PostgreSQL catalog row; operator repair required') from error
        finally:conn.close()
    def import_legacy(self)->dict[str,int]:
        """Stage unreviewed pairing suggestions and preserve stable IDs/provenance.

        A newly named wine after first import requires explicit identity review;
        that prevents rename/spelling corrections silently discarding enrichments.
        The full migration is atomic, while public.wines is never written.
        """
        conn=self.connection_factory()
        try:
            # Full authoritative snapshot: source venue registry is the scope of
            # reconciliation. A registered venue with zero wine rows MUST clear
            # stale staging rows, without touching unrelated reference venues.
            # The registry read, bounded source stream and all destination writes
            # share one PostgreSQL transaction so failures roll back together.
            with conn:
                # Prevent reads from observing two different committed legacy
                # source states across the registry and wine SELECTs.
                with conn.cursor() as isolation:
                    isolation.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
                with conn.cursor(name='palate_legacy_registry') as registry:
                    registry.execute('SELECT id FROM public.restaurants ORDER BY id')
                    restaurant_rows=registry.fetchmany(MAX_CATALOG_ROWS+1)
                if len(restaurant_rows)>MAX_CATALOG_ROWS:
                    raise CatalogDataError('Unexpected legacy venue count')
                registry_ids=[row[0] for row in restaurant_rows]
                if len(set(registry_ids))!=len(registry_ids):
                    raise CatalogDataError('Duplicate legacy restaurant ID')
                venues={'legacy-'+str(rest_id):[] for rest_id in registry_ids}
                with conn.cursor(name='palate_legacy_bounded_import') as source:
                    source.execute("""SELECT restaurant_id,wine_name,available_by_glass,available_by_bottle,origin,category,
                     type,characteristics,body,sweet_or_dry,general_pairings,metadata FROM public.wines ORDER BY restaurant_id,wine_name""")
                    old_rows=source.fetchmany(MAX_CATALOG_ROWS+1)
                if len(old_rows)>MAX_CATALOG_ROWS:
                    raise CatalogDataError('Unexpected legacy inventory size')
                for row in old_rows:
                    wine=legacy_row_to_wine(row)
                    if wine.restaurant_id not in venues:
                        raise CatalogDataError('Wine references a venue missing from trusted source registry')
                    venues[wine.restaurant_id].append((wine,row))
                # Reconcile removed authoritative venues in the same transaction.
                # The legacy-N numeric namespace is reserved for this importer;
                # never touch curated/non-legacy venues or reviewed identity maps.
                # Preserve the inactive venue and aliases for audit/re-enrollment,
                # but remove its stale recommendation inventory immediately.
                with conn.cursor() as cur:
                    cur.execute("""SELECT venue_id FROM portfolio_v2.venues
                        WHERE venue_id ~ '^legacy-[0-9]+$' ORDER BY venue_id FOR UPDATE""")
                    staged_legacy_ids=[row[0] for row in cur.fetchmany(MAX_CATALOG_ROWS+1)]
                    if len(staged_legacy_ids)>MAX_CATALOG_ROWS:
                        raise CatalogDataError('Unexpected staged legacy venue count')
                    for stale_id in staged_legacy_ids:
                        # Defense in depth: scope stays numeric legacy IDs even
                        # if a connection adapter returns unexpected rows.
                        if not isinstance(stale_id,str) or not re.fullmatch(r'legacy-[0-9]+',stale_id):
                            raise CatalogDataError('Staged legacy registry query returned non-legacy identity')
                        if stale_id not in venues:
                            self._archive_active(cur,stale_id)
                            cur.execute('DELETE FROM portfolio_v2.wines WHERE venue_id=%s',(stale_id,))
                    for venue_id,records in venues.items():
                        cur.execute('INSERT INTO portfolio_v2.venues(venue_id,display_name) VALUES (%s,%s) ON CONFLICT(venue_id) DO NOTHING',(venue_id,venue_id))
                        cur.execute('SELECT venue_id FROM portfolio_v2.venues WHERE venue_id=%s FOR UPDATE',(venue_id,))
                        cur.execute('SELECT legacy_name,wine_id FROM portfolio_v2.legacy_identity_map WHERE venue_id=%s ORDER BY legacy_name LIMIT %s',(venue_id,MAX_ALIAS_ROWS+1))
                        alias_rows=cur.fetchmany(MAX_ALIAS_ROWS+1)
                        if len(alias_rows)>MAX_ALIAS_ROWS:
                            raise CatalogDataError('Legacy identity history exceeds configured row limit')
                        aliases={r[0]:r[1] for r in alias_rows}
                        cur.execute("""SELECT wine_id,wine_name,pronunciation,characteristics,pairings,
                                      pairings_reviewed FROM portfolio_v2.wines WHERE venue_id=%s ORDER BY wine_id LIMIT %s""",(venue_id,MAX_CATALOG_ROWS+1))
                        old_rows=cur.fetchmany(MAX_CATALOG_ROWS+1)
                        if len(old_rows)>MAX_CATALOG_ROWS:
                            raise CatalogDataError('Staged legacy inventory exceeds administrative row limit')
                        old={r[0]:r[1:] for r in old_rows}
                        self._archive_active(cur,venue_id)
                        archived=self._read_archive(cur,venue_id)
                        # If prior catalog exists but mapping was never initialized,
                        # require explicit operator review rather than guessing.
                        if records and old and not aliases:raise CatalogDataError('Legacy identity map must be reviewed before import')
                        resolved=[];seen_ids=set()
                        for w,row in records:
                            alias=self._legacy_alias(w.wine_name)
                            if alias in aliases:
                                wine_id=aliases[alias]
                            elif aliases:
                                raise CatalogDataError('Unrecognized legacy wine name requires manual ID review: '+alias)
                            else:
                                wine_id=w.wine_id
                                cur.execute("""INSERT INTO portfolio_v2.legacy_identity_map(venue_id,legacy_name,wine_id,reviewed)
                                  VALUES (%s,%s,%s,FALSE)""",(venue_id,alias,wine_id))
                                aliases[alias]=wine_id
                            if wine_id in seen_ids:raise CatalogDataError('Two legacy names resolve to same wine ID')
                            seen_ids.add(wine_id)
                            resolved.append((w,row,wine_id))
                        cur.execute('DELETE FROM portfolio_v2.wines WHERE venue_id=%s',(venue_id,))
                        for w,row,wine_id in resolved:
                            prior=old.get(wine_id)
                            saved=archived.get(wine_id)
                            pronunciation=restored_pronunciation(prior[1] if prior else None,
                                                                 saved[0] if saved else None,w.pronunciation)
                            old_tags=self._json_tags(prior[2]) if prior else self._json_tags(saved[1]) if saved else []
                            old_pairings=self._json_tags(prior[3]) if prior else self._json_tags(saved[2]) if saved else []
                            reviewed=bool(prior[4]) if prior else bool(saved[3]) if saved else False
                            active=old_pairings if reviewed else []
                            staged=sorted(extract_reviewable_pairings(row[10]))
                            cur.execute("""INSERT INTO portfolio_v2.wines(venue_id,wine_id,wine_name,pronunciation,origin,category,wine_type,sweetness,
                             characteristics,pairings,available_by_glass,available_by_bottle,source_metadata,source_characteristics,source_body,source_pairings,
                             suggested_pairings,pairings_reviewed)
                             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)""",
                             (venue_id,wine_id,w.wine_name,pronunciation,w.origin,w.category,w.type,w.sweetness,
                              json.dumps(sorted(set(old_tags)|set(w.characteristics))),json.dumps(sorted(active)),w.available_by_glass,w.available_by_bottle,
                              *(row[k] if row[k] is not None else saved[4+i] if saved else None
                                for i,k in enumerate((11,7,8,10))),
                              json.dumps(staged),reviewed))
            return {key:len(rows) for key,rows in venues.items()}
        finally:conn.close()

def connect_from_env(*,dsn:str|None=None,environ=None):
    import os
    try:import psycopg2
    except ImportError as e:raise RuntimeError('Install psycopg2-binary for PostgreSQL integration') from e
    dsn=dsn if dsn is not None else (environ if environ is not None else os.environ).get('PG_DSN')
    if not isinstance(dsn,str) or not dsn.strip():raise RuntimeError('PG_DSN required')
    # Explicit kwargs override DSN/libpq environment defaults. For remote TCP,
    # verify-full checks both certificate chain and the destination hostname;
    # never fall back to cleartext or prefer GSSAPI over verified TLS.
    # libpq ignores sslmode for local Unix-domain sockets.
    return lambda:psycopg2.connect(dsn,connect_timeout=10,
                                   sslmode='verify-full',gssencmode='disable')
