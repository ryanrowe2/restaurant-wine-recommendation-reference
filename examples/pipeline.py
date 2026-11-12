"""Independent, sanitized Restaurant Wine Reference reference implementation.

Domain models, bounded catalog parsing, deterministic fallback/ranking and exact
contract validation. This code is not former organization's historical application.
"""
from __future__ import annotations
import csv
import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

CATALOG_PATH = Path(__file__).with_name('synthetic_wines.csv')
REQUIRED_FIELDS = ('wine_id','wine_name','pronunciation','origin','type','description')
CATALOG_COLUMNS = ('restaurant_id','wine_id','wine_name','pronunciation','origin','category','type','sweetness','characteristics','available_by_glass','available_by_bottle','pairings')
CATEGORIES = frozenset(('red','white','sparkling','rose'))
SWEETNESS = frozenset(('dry','semi-dry','semi-sweet','sweet'))
SERVING = frozenset(('glass','bottle','either'))
MAX_CATALOG_BYTES = 2_000_000
MAX_CATALOG_ROWS = 10_000
MAX_FIELD_LENGTH = 1500
MAX_TAGS = 128
MAX_TAG_LENGTH = 48
MAX_PREF_LENGTH = 64
MAX_DESCRIPTION = 1500

class InsufficientCandidates(ValueError):
    """Not enough unique wines at a venue to fulfil an exactly-three request."""
class ContractError(ValueError):
    """Generated text failed catalog-bound output validation."""
class CatalogDataError(ValueError):
    """Source catalog is malformed (operational failure)."""
class InputError(ValueError):
    """External request/preferences are invalid."""

# Additional default-ignorable non-Cf Unicode format codepoints that can
# produce visually empty or deceptively rendered catalog labels.
# The committed PostgreSQL migration uses a fixed Unicode codepoint set.
# Some Cf codepoints were added to Python's Unicode tables after Python 3.10;
# preserve the same rejection rule on older interpreters without weakening it.
DEFAULT_IGNORABLE_POINTS = frozenset((
    0x034F,0x061C,0x0890,0x0891,0x115F,0x1160,0x17B4,0x17B5,0x3164,0xFFA0,
    0x13439,0x1343A,0x1343B,0x1343C,0x1343D,0x1343E,0x1343F,
))
DEFAULT_IGNORABLE_RANGES = ((0x180B,0x180F),(0xFE00,0xFE0F),(0xE0100,0xE01EF))

def valid_venue_id(value: object) -> bool:
    return valid_required_wine_text(value) and len(value)<=128

def valid_required_wine_text(value: object) -> bool:
    """Shared Wine/SQLite domain rule; PostgreSQL v15 SQL mirrors this set.

    Only ordinary ASCII space is permitted as whitespace. UTF-8 validity and
    all Unicode Cc controls are checked before output or persistence.
    """
    if type(value) is not str or not 1 <= len(value) <= MAX_FIELD_LENGTH:
        return False
    if not value.strip() or any(unicodedata.category(c) in ('Cc','Cf') or
                                ord(c) in DEFAULT_IGNORABLE_POINTS or
                                any(lo<=ord(c)<=hi for lo,hi in DEFAULT_IGNORABLE_RANGES) or
                                (c.isspace() and c!=' ') for c in value):
        return False
    try:
        value.encode('utf-8','strict')
    except UnicodeError:
        return False
    return True

def normalize_text(value: str) -> str:
    if not isinstance(value,str): raise InputError('Expected string')
    return ' '.join(value.strip().split()).lower()

def normalize_sweetness(value: str) -> str:
    return normalize_text(value).replace('_','-')

def _clean_tags(tags: Iterable[str], *, max_tags: int=MAX_TAGS) -> tuple[str,...]:
    if not isinstance(tags,(tuple,list,set,frozenset)):
        raise InputError('Tags must be a sequence')
    if len(tags) > max_tags: raise InputError('Too many preference tags')
    out = []
    for tag in tags:
        cleaned = normalize_text(tag)
        if len(cleaned) > MAX_TAG_LENGTH or (cleaned and not re.fullmatch(r'[a-z0-9_ -]+',cleaned)):
            raise InputError('Invalid tag')
        if cleaned: out.append(cleaned.replace(' ','_'))
    return tuple(sorted(set(out)))

@dataclass(frozen=True)
class Wine:
    restaurant_id: str
    wine_id: str
    wine_name: str
    pronunciation: str
    origin: str
    category: str
    type: str
    sweetness: str
    characteristics: frozenset[str]
    available_by_glass: bool
    available_by_bottle: bool
    pairings: frozenset[str]
    def __post_init__(self):
        for field in ('restaurant_id','wine_id','wine_name','pronunciation','origin','type'):
            value=getattr(self,field)
            if not valid_required_wine_text(value):
                raise CatalogDataError('Missing or invalid required wine field: '+field)
        if not valid_venue_id(self.restaurant_id):
            raise CatalogDataError('Invalid venue identity')
        if not isinstance(self.category,str) or normalize_text(self.category) not in CATEGORIES:
            raise CatalogDataError('Invalid wine category')
        if not isinstance(self.sweetness,str) or normalize_sweetness(self.sweetness) not in SWEETNESS:
            raise CatalogDataError('Invalid wine sweetness')
        if type(self.available_by_glass) is not bool or type(self.available_by_bottle) is not bool or not (self.available_by_glass or self.available_by_bottle):
            raise CatalogDataError('Wine must have at least one valid available format')
        for key in ('characteristics','pairings'):
            v=getattr(self,key)
            if not isinstance(v,frozenset): raise CatalogDataError('Tags must be frozensets')
            try: _clean_tags(v)
            except InputError as e: raise CatalogDataError(str(e)) from e
        object.__setattr__(self,'category',normalize_text(self.category))
        object.__setattr__(self,'sweetness',normalize_sweetness(self.sweetness))
        object.__setattr__(self,'characteristics',frozenset(_clean_tags(self.characteristics)))
        object.__setattr__(self,'pairings',frozenset(_clean_tags(self.pairings)))

@dataclass(frozen=True)
class Preferences:
    category: str=''
    sweetness: str=''
    characteristics: tuple[str,...]=()
    serving_format: str='either'
    pairing: str=''
    def __post_init__(self):
        for field in ('category','sweetness','serving_format','pairing'):
            value=getattr(self,field)
            if not isinstance(value,str) or len(value)>MAX_PREF_LENGTH:
                raise InputError('Invalid preferences: '+field)
        category=normalize_text(self.category)
        sweetness=normalize_sweetness(self.sweetness)
        serving=normalize_text(self.serving_format)
        pairing=normalize_text(self.pairing).replace(' ','_')
        if serving not in SERVING: raise InputError('Invalid serving format')
        # Unfamiliar *preferences* can be progressively relaxed. Catalog values cannot.
        if category and not re.fullmatch(r'[a-z_]{1,64}',category): raise InputError('Invalid category')
        if sweetness and not re.fullmatch(r'[a-z_-]{1,64}',sweetness): raise InputError('Invalid sweetness')
        if pairing and not re.fullmatch(r'[a-z0-9_]{1,64}',pairing): raise InputError('Invalid pairing')
        tags=_clean_tags(self.characteristics)
        object.__setattr__(self,'category',category)
        object.__setattr__(self,'sweetness',sweetness)
        object.__setattr__(self,'serving_format',serving)
        object.__setattr__(self,'pairing',pairing)
        object.__setattr__(self,'characteristics',tags)

@dataclass(frozen=True)
class Selection:
    wines: tuple[Wine,...]
    stages_visited: tuple[str,...]
    priority_tiers: tuple[int,...]=()

def load_catalog(path: Path=CATALOG_PATH)->tuple[Wine,...]:
    path=Path(path)
    if path.stat().st_size>MAX_CATALOG_BYTES: raise CatalogDataError('Catalog exceeds file size limit')
    wines=[]; identities=set()
    with path.open(encoding='utf-8-sig',newline='') as file:
        reader=csv.DictReader(file)
        if reader.fieldnames is None or len(reader.fieldnames)!=len(CATALOG_COLUMNS) or len(set(reader.fieldnames))!=len(reader.fieldnames) or set(reader.fieldnames)!=set(CATALOG_COLUMNS):
            raise CatalogDataError('Catalog header does not match schema')
        for line,row in enumerate(reader,start=2):
            if len(wines)>=MAX_CATALOG_ROWS: raise CatalogDataError('Catalog exceeds record limit')
            if None in row or any(not isinstance(x,str) or len(x)>MAX_FIELD_LENGTH for x in row.values()):
                raise CatalogDataError(f'Malformed catalog row {line}')
            row={key:value.strip() for key,value in row.items()}
            identity=(row['restaurant_id'],row['wine_id'])
            if identity in identities: raise CatalogDataError(f'Missing or duplicate scoped wine identity: {identity}')
            identities.add(identity)
            for flag in ('available_by_glass','available_by_bottle'):
                if row[flag].lower() not in ('yes','no'): raise CatalogDataError(f'Unrecognized {flag} for {identity}')
            try:
                wine=Wine(restaurant_id=row['restaurant_id'],wine_id=row['wine_id'],wine_name=row['wine_name'],pronunciation=row['pronunciation'],origin=row['origin'],category=row['category'],type=row['type'],sweetness=row['sweetness'],characteristics=frozenset(row['characteristics'].split(';')) if row['characteristics'] else frozenset(),available_by_glass=row['available_by_glass'].lower()=='yes',available_by_bottle=row['available_by_bottle'].lower()=='yes',pairings=frozenset(row['pairings'].split(';')) if row['pairings'] else frozenset())
            except (InputError,CatalogDataError) as error: raise CatalogDataError(f'Row {line}: {error}') from error
            wines.append(wine)
    if not wines: raise CatalogDataError('A catalog cannot be empty')
    return tuple(wines)

def format_eligible(wine: Wine,requested_format: str)->bool:
    if not isinstance(wine,Wine): raise CatalogDataError('Unvalidated wine object')
    if requested_format=='glass': return wine.available_by_glass
    if requested_format=='bottle': return wine.available_by_bottle
    if requested_format=='either': return wine.available_by_glass or wine.available_by_bottle
    raise InputError('Invalid serving format')

def _matches(wine:Wine,category:str,sweetness:str,descriptors:frozenset[str],mode:str)->bool:
    if category and wine.category!=category:return False
    if sweetness and wine.sweetness!=sweetness:return False
    if mode=='all':return descriptors.issubset(wine.characteristics)
    if mode=='any':return bool(descriptors&wine.characteristics)
    return True

def select_candidates(catalog:Iterable[Wine],restaurant_id:str,preferences:Preferences,*,required:int=3,max_candidates:int=3)->Selection:
    if not valid_venue_id(restaurant_id): raise InputError('Invalid restaurant ID')
    if not isinstance(preferences,Preferences): raise InputError('Invalid preferences')
    if required!=3 or type(max_candidates) is not int or not 3<=max_candidates<=12: raise InputError('Exactly three output; candidate pool bounded to 3-12')
    scoped={}
    for wine in catalog:
        if not isinstance(wine,Wine):raise CatalogDataError('Catalog contained an unvalidated record')
        if wine.restaurant_id==restaurant_id and format_eligible(wine,preferences.serving_format):
            if wine.wine_id in scoped: raise CatalogDataError('Duplicate scoped wine ID')
            scoped[wine.wine_id]=wine
    descriptors=frozenset(preferences.characteristics)
    plan=[]
    # Preserve the requested sweetness even with zero descriptor tags.
    # Strict sweetness is always preferred; adjacent styles are explicit fallback tiers.
    adjacent={'dry':'semi-dry','sweet':'semi-sweet','semi-dry':'dry','semi-sweet':'sweet'}
    sweet_stages=tuple(dict.fromkeys((preferences.sweetness,adjacent.get(preferences.sweetness,''),'')))
    for cat in dict.fromkeys((preferences.category,'')):
        for sweet in sweet_stages:
            if descriptors: plan.extend(((cat,sweet,'all'),(cat,sweet,'any')))
            plan.append((cat,sweet,'none'))
    first_stage={}; visited=[]
    for stage_num,(cat,sweet,mode) in enumerate(dict.fromkeys(plan)):
        visited.append(f'category={cat or "*"};sweetness={sweet or "*"};descriptor_match={mode}')
        for wine in scoped.values():
            if wine.wine_id not in first_stage and _matches(wine,cat,sweet,descriptors,mode):first_stage[wine.wine_id]=stage_num
        if len(first_stage)>=required:break
    if len(first_stage)<required:
        raise InsufficientCandidates(f'Only {len(first_stage)} distinct eligible wines for the venue and serving format')
    # Rank across the full eligible result set at the reached stage; do not truncate by ID.
    ranked=sorted((scoped[wid] for wid in first_stage),key=lambda w:(first_stage[w.wine_id],-int(bool(preferences.pairing) and preferences.pairing in w.pairings),-len(descriptors&w.characteristics),w.wine_id))
    selected=tuple(ranked[:max_candidates])
    return Selection(selected,tuple(visited),tuple(first_stage[w.wine_id] for w in selected))

def validate_selection_priority(recommendations:object,selection:Selection)->None:
    """Never let model ranking discard a higher-priority fallback tier.

    At most three distinct catalog wines are returned. If a stricter tier has
    fewer than three wines, every one must be retained; otherwise the model
    may order/choose freely *within* the best available tier.
    """
    if not selection.priority_tiers:
        return  # Independently-constructed Selection has no planner evidence.
    if (len(selection.priority_tiers)!=len(selection.wines)
            or not isinstance(recommendations,list) or len(recommendations)!=3):
        raise ContractError('Missing selection-priority evidence')
    by_id={w.wine_id:tier for w,tier in zip(selection.wines,selection.priority_tiers)}
    if len(by_id)!=len(selection.wines):
        raise ContractError('Duplicate candidate identity')
    candidate_tiers=sorted(by_id.values())
    if len(candidate_tiers)<3:raise ContractError('Insufficient selection tiers')
    boundary=candidate_tiers[2]
    required={wid for wid,tier in by_id.items() if tier<boundary}
    picked={rec['wine_id'] for rec in recommendations if isinstance(rec,dict) and isinstance(rec.get('wine_id'),str)}
    if not required.issubset(picked) or any(by_id.get(wid,10**12)>boundary for wid in picked):
        raise ContractError('Ranking discards a strictly preferred candidate tier')


def create_deterministic_recommendations(selection:Selection,preferences:Preferences)->list[dict[str,str]]:
    if len(selection.wines)<3:raise InsufficientCandidates('Three candidates are required')
    if not isinstance(preferences,Preferences):raise InputError('Invalid preferences')
    output=[]
    for wine in selection.wines[:3]:
        output.append({'wine_id':wine.wine_id,'wine_name':wine.wine_name,'pronunciation':wine.pronunciation,'origin':wine.origin,'type':wine.type,'description':_trusted_description(wine,preferences)})
    return output

def _trusted_description(wine:Wine,preferences:Preferences)->str:
    matched=[d for d in preferences.characteristics if d in wine.characteristics]
    context=(f'Your {preferences.pairing.replace("_"," ")} pairing matches this wine.'
             if preferences.pairing and preferences.pairing in wine.pairings
             else 'This wine is available in the selected serving format.')
    if matched:context+=' It matches your '+', '.join(matched[:12])+' preference.'
    else:context+=f' Catalog sweetness: {wine.sweetness}.'
    return context

def validate_recommendation_contract(recommendations:object,eligible:Iterable[Wine],restaurant_id:str|None=None,
                                     *,preferences:Preferences|None=None)->None:
    if not isinstance(recommendations,list) or len(recommendations)!=3:raise ContractError('Expected exactly three recommendations')
    members={}; venue_set=set()
    for wine in eligible:
        if not isinstance(wine,Wine):raise ContractError('Unvalidated eligible wine')
        venue_set.add(wine.restaurant_id)
        identity=(wine.restaurant_id,wine.wine_id)
        if identity in members: raise ContractError('Duplicate eligible identity')
        members[identity]=wine
    if len(venue_set)!=1:raise ContractError('Mixed or empty venue candidate set')
    scope=next(iter(venue_set))
    if restaurant_id is not None and restaurant_id!=scope:raise ContractError('Venue authorization mismatch')
    if len(members)<3:raise ContractError('Candidate set too small')
    seen=set()
    for record in recommendations:
        if not isinstance(record,dict) or set(record)!=set(REQUIRED_FIELDS):raise ContractError('Unexpected or missing response fields')
        if any(not isinstance(record[f],str) or not record[f].strip() for f in REQUIRED_FIELDS):raise ContractError('Invalid response field')
        for f in REQUIRED_FIELDS:
            if len(record[f])>(MAX_DESCRIPTION if f=='description' else MAX_FIELD_LENGTH):raise ContractError('Response text too long')
            if any(ord(c)<32 and c not in '\t\n' for c in record[f]):raise ContractError('Forbidden control character')
        key=(scope,record['wine_id'])
        if key in seen or key not in members:raise ContractError('Duplicate or ineligible wine')
        wine=members[key]
        if any(record[f]!=getattr(wine,f) for f in ('wine_name','pronunciation','origin','type')):raise ContractError('Catalog attribute mismatch')
        if preferences is not None and record['description']!=_trusted_description(wine,preferences):
            raise ContractError('Customer-facing description is not grounded in trusted template')
        seen.add(key)

def run_example()->dict[str,object]:
    preferences=Preferences(category='red',sweetness='dry',characteristics=('bold','spice'),serving_format='glass',pairing='red_meat')
    selection=select_candidates(load_catalog(),'venue_a',preferences)
    recommendations=create_deterministic_recommendations(selection,preferences)
    validate_recommendation_contract(recommendations,selection.wines,'venue_a')
    return {'venue':'venue_a','mode':'offline_synthetic','stages_visited':selection.stages_visited,'recommendations':recommendations}

if __name__=='__main__': print(json.dumps(run_example(),indent=2))
