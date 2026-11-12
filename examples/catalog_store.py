"""Transactional full-record catalog store using fictional data and SQLite."""
from __future__ import annotations
import json
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from itertools import islice
from typing import Iterable
from dbutil import SQLiteDatabase,assert_canonical_table
from pipeline import Wine, CatalogDataError, MAX_CATALOG_ROWS, MAX_TAGS, MAX_TAG_LENGTH, MAX_FIELD_LENGTH, valid_required_wine_text, valid_venue_id

MAX_TAG_JSON_BYTES = 2 + MAX_TAGS * (MAX_TAG_LENGTH + 4)  # bound any stored JSON representation

class CatalogStore:
    @staticmethod
    def _valid_required_text(value):
        """SQLite UDF, used by the persisted table's fail-closed triggers.

        Unlike SQL length(), Python checks past embedded NUL bytes, and the
        connection's UTF-8 decoder rejects malformed TEXT before this runs.
        """
        return int(valid_required_wine_text(value))

    @staticmethod
    def _valid_tag_array(raw):
        """Full-value SQLite UDF: JSON containers, members and code points.

        SQLite length(), GLOB and LIKE stop or misclassify embedded NUL bytes;
        validating decoded Python strings also covers Unicode escapes. A raw
        connection lacking this UDF fails closed on INSERT and UPDATE.
        """
        if type(raw) is not str or len(raw)>MAX_TAG_JSON_BYTES:
            return 0
        try:
            if len(raw.encode('utf-8','strict')) > MAX_TAG_JSON_BYTES:
                return 0
            tags=json.loads(raw)
            if type(tags) is not list or len(tags)>MAX_TAGS:
                return 0
            if tags!=sorted(set(tags)) or raw!=json.dumps(tags,ensure_ascii=True):
                return 0
            for tag in tags:
                if type(tag) is not str or re.fullmatch(r'[a-z0-9_-]{1,'+str(MAX_TAG_LENGTH)+r'}',tag) is None:
                    return 0
                tag.encode('utf-8',errors='strict')
        except (ValueError,TypeError,UnicodeError,RecursionError):
            return 0
        return 1

    def __init__(self,path:Path|str):
        self._db=SQLiteDatabase(path)
        with self._connect() as conn:
            schema='''
             CREATE TABLE IF NOT EXISTS restaurants (
               restaurant_id TEXT PRIMARY KEY CHECK(length(restaurant_id)>0));
             CREATE TABLE IF NOT EXISTS catalog (
               restaurant_id TEXT NOT NULL REFERENCES restaurants(restaurant_id),
               wine_id TEXT NOT NULL CHECK(length(wine_id)>0),
               wine_name TEXT NOT NULL, pronunciation TEXT NOT NULL,
               origin TEXT NOT NULL, category TEXT NOT NULL CHECK(category IN ('red','white','sparkling','rose')),
               type TEXT NOT NULL,
               sweetness TEXT NOT NULL CHECK(sweetness IN ('dry','semi-dry','semi-sweet','sweet')),
               characteristics TEXT NOT NULL CHECK(json_valid(characteristics) AND json_type(characteristics)='array'),
               pairings TEXT NOT NULL CHECK(json_valid(pairings) AND json_type(pairings)='array'),
               available_by_glass INTEGER NOT NULL CHECK(available_by_glass IN(0,1)),
               available_by_bottle INTEGER NOT NULL CHECK(available_by_bottle IN(0,1)),
               CHECK(available_by_glass=1 OR available_by_bottle=1),
               PRIMARY KEY (restaurant_id,wine_id));
             CREATE INDEX IF NOT EXISTS catalog_venue_category ON catalog(restaurant_id,category,sweetness);
            '''
            # Refuse unknown persistent behavior: an AFTER trigger can silently
            # change a row *after* all pre-insert validators have approved it.
            expected_triggers={f'catalog_{kind}_{action}' for kind in ('validate_tags','required')
                               for action in ('insert','update')}
            existing_triggers={name for (name,) in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name IN ('catalog','restaurants')")}
            if existing_triggers-expected_triggers:
                raise ValueError('Unsafe unexpected SQLite catalog trigger: '+
                                 ', '.join(sorted(existing_triggers-expected_triggers)))
            existing_indexes={name for (name,) in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name IN ('catalog','restaurants')")
                if not name.startswith('sqlite_autoindex_')}
            if existing_indexes-{'catalog_venue_category'}:
                raise ValueError('Unsafe unexpected SQLite catalog index')
            index_ddl=conn.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name='catalog_venue_category'").fetchone()
            if index_ddl:
                canonical=lambda sql: re.sub(r'\s+','',re.sub(r'\bIF\s+NOT\s+EXISTS\b','',sql,flags=re.I)).lower()
                if canonical(index_ddl[0]) != canonical('CREATE INDEX catalog_venue_category ON catalog(restaurant_id,category,sweetness)'):
                    raise ValueError('Unsafe historical SQLite catalog index drift')
            # Check existing tables before creating indexes or altering triggers.
            # Otherwise a weak old schema could fail in a later DDL statement
            # without a clear, deterministic schema-integrity diagnosis.
            table_ddls={}
            for ddl in schema.split(';'):
                for table in ('restaurants','catalog'):
                    if f'CREATE TABLE IF NOT EXISTS {table} ' in ddl:
                        table_ddls[table]=ddl
                        if conn.execute("SELECT 1 FROM sqlite_master WHERE name=?",(table,)).fetchone():
                            assert_canonical_table(conn,table,ddl)
            # DDL and integrity checks must be atomic. executescript() silently
            # commits any pending SQLite transaction before running a script.
            conn.execute('BEGIN IMMEDIATE')
            table_ddls={}
            for ddl in schema.split(';'):
                if ddl.strip():
                    conn.execute(ddl)
                    for table in ('restaurants','catalog'):
                        if f'CREATE TABLE IF NOT EXISTS {table} ' in ddl:
                            table_ddls[table]=ddl
            for table,ddl in table_ddls.items():
                assert_canonical_table(conn,table,ddl)
            # Always replace old definitions, never retain a permissive trigger
            # from v13 or an earlier deployment. All changes share one transaction.
            for action in ('INSERT','UPDATE'):
                trigger=f'catalog_validate_tags_{action.lower()}'
                conn.execute(f'DROP TRIGGER IF EXISTS {trigger}')
                conn.execute(f"""CREATE TRIGGER {trigger}
                 BEFORE {action} ON catalog BEGIN
                   SELECT RAISE(ABORT,'invalid catalog tag array') WHERE
                     palate_valid_tag_array(NEW.characteristics)!=1 OR
                     palate_valid_tag_array(NEW.pairings)!=1;
                 END""")
            # Rebuild old triggers on initialization: CREATE IF NOT EXISTS
            # would leave v12's NUL/UTF-8 bypass in existing databases.
            required_fields=('restaurant_id','wine_id','wine_name','pronunciation','origin','type')
            for action in ('INSERT','UPDATE'):
                conn.execute(f'DROP TRIGGER IF EXISTS catalog_required_{action.lower()}')
                condition=' OR '.join(
                    f'palate_valid_required_text(NEW.{field}) != 1'
                    for field in required_fields)
                conn.execute(f"""CREATE TRIGGER catalog_required_{action.lower()}
                  BEFORE {action} ON catalog BEGIN
                    SELECT RAISE(ABORT,'invalid required catalog text') WHERE {condition};
                  END""")
            # On upgrade, reject already-invalid persisted values rather than
            # silently accepting their former trigger validation.
            where=' OR '.join(f'palate_valid_required_text({field}) != 1'
                              for field in required_fields)
            try:
                invalid=conn.execute(f'SELECT 1 FROM catalog WHERE {where} LIMIT 1').fetchone()
            except (sqlite3.DatabaseError,UnicodeError) as ex:
                raise CatalogDataError('Persisted catalog text cannot be decoded or validated') from ex
            if invalid:
                raise CatalogDataError('Persisted catalog contains invalid required text')
            try:
                bad_tags=conn.execute('''SELECT 1 FROM catalog WHERE
                    palate_valid_tag_array(characteristics)!=1 OR
                    palate_valid_tag_array(pairings)!=1 LIMIT 1''').fetchone()
            except (sqlite3.DatabaseError,UnicodeError) as ex:
                raise CatalogDataError('Persisted catalog tags cannot be decoded or validated') from ex
            if bad_tags:
                raise CatalogDataError('Persisted catalog contains invalid tags')
    @staticmethod
    def _tags(raw):
        if CatalogStore._valid_tag_array(raw)!=1:
            raise CatalogDataError('Stored catalog tags must be canonical, bounded string arrays')
        return frozenset(json.loads(raw))
    @contextmanager
    def _connect(self):
        with self._db.connect() as conn:
            # Required for any connection that writes into catalog. An external
            # SQLite writer missing this UDF fails closed, never bypasses it.
            conn.create_function('palate_valid_required_text',1,
                                 self._valid_required_text,deterministic=True)
            conn.create_function('palate_valid_tag_array',1,
                                 self._valid_tag_array,deterministic=True)
            yield conn
    def close(self):self._db.close()
    def replace_venue(self,venue_id:str,wines:Iterable[Wine],*,allow_empty:bool=False)->int:
        # Inspect one element past the cap, never exhaust an untrusted generator.
        incoming=tuple(islice(wines,MAX_CATALOG_ROWS+1))
        if (not valid_venue_id(venue_id)
            or type(allow_empty) is not bool or len(incoming)>MAX_CATALOG_ROWS
            or (not incoming and not allow_empty)):
            raise ValueError('A valid venue and bounded authorized snapshot are required')
        seen=set()
        for wine in incoming:
            if not isinstance(wine,Wine) or wine.restaurant_id!=venue_id:raise ValueError('Invalid or cross-venue inventory row')
            if wine.wine_id in seen:raise ValueError('Duplicate wine identity in incoming catalog')
            seen.add(wine.wine_id)
        with self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            if not incoming:
                registered=conn.execute('SELECT 1 FROM restaurants WHERE restaurant_id=?',(venue_id,)).fetchone()
                if registered is None:
                    raise ValueError('Empty replacement requires an existing registered venue')
            else:
                conn.execute('INSERT OR IGNORE INTO restaurants(restaurant_id) VALUES (?)',(venue_id,))
            conn.execute('DELETE FROM catalog WHERE restaurant_id=?',(venue_id,))
            conn.executemany('''INSERT INTO catalog (
              restaurant_id,wine_id,wine_name,pronunciation,origin,category,type,sweetness,characteristics,pairings,
              available_by_glass,available_by_bottle) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',[
                (w.restaurant_id,w.wine_id,w.wine_name,w.pronunciation,w.origin,w.category,w.type,w.sweetness,
                 json.dumps(sorted(w.characteristics)),json.dumps(sorted(w.pairings)),int(w.available_by_glass),int(w.available_by_bottle))
                for w in incoming])
            # Validate the *persisted* snapshot while still holding the write
            # transaction. Any trigger-driven rewrite must abort the replacement.
            actual=conn.execute("""SELECT restaurant_id,wine_id,wine_name,pronunciation,origin,category,type,
                sweetness,characteristics,pairings,available_by_glass,available_by_bottle
                FROM catalog WHERE restaurant_id=? ORDER BY wine_id""",(venue_id,)).fetchall()
            expected=sorted(((w.restaurant_id,w.wine_id,w.wine_name,w.pronunciation,w.origin,w.category,w.type,
                w.sweetness,json.dumps(sorted(w.characteristics)),json.dumps(sorted(w.pairings)),
                int(w.available_by_glass),int(w.available_by_bottle)) for w in incoming),key=lambda row:row[1])
            if actual!=expected:
                raise CatalogDataError('Persisted catalog differs from reviewed inventory snapshot')
        return len(incoming)
    def wines_for_venue(self,venue_id:str)->tuple[Wine,...]:
        try:
            with self._connect() as conn:
                rows=conn.execute('''SELECT restaurant_id,wine_id,wine_name,pronunciation,origin,category,type,sweetness,
             characteristics,pairings,available_by_glass,available_by_bottle FROM catalog WHERE restaurant_id=? ORDER BY wine_id LIMIT ?''',(venue_id,MAX_CATALOG_ROWS+1)).fetchall()
        except (sqlite3.DatabaseError,UnicodeError) as error:
            raise CatalogDataError('Stored SQLite catalog cannot be decoded') from error
        if len(rows)>MAX_CATALOG_ROWS:
            raise CatalogDataError('Persisted SQLite catalog exceeds row limit')
        try:
            return tuple(Wine(restaurant_id=r[0],wine_id=r[1],wine_name=r[2],pronunciation=r[3],origin=r[4],category=r[5],type=r[6],sweetness=r[7],characteristics=self._tags(r[8]),pairings=self._tags(r[9]),available_by_glass=bool(r[10]),available_by_bottle=bool(r[11])) for r in rows)
        except (ValueError,TypeError) as error:raise CatalogDataError('Stored catalog invalid') from error
    def has_venue(self,venue_id:str)->bool:
        with self._connect() as conn:
            return conn.execute('SELECT 1 FROM restaurants WHERE restaurant_id=?',(venue_id,)).fetchone() is not None
    def has_minimum_inventory(self,venue_id:str,minimum:int=3)->bool:
        if type(minimum) is not int or minimum<1 or minimum>MAX_CATALOG_ROWS:
            raise ValueError('Invalid inventory threshold')
        with self._connect() as conn:
            rows=conn.execute('SELECT 1 FROM catalog WHERE restaurant_id=? LIMIT ?',(venue_id,minimum)).fetchall()
        return len(rows)==minimum
    def names_for_venue(self,venue_id:str)->tuple[str,...]:
        return tuple(w.wine_name for w in self.wines_for_venue(venue_id))
