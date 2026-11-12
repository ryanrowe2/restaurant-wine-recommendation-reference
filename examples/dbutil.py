"""Scoped SQLite connection helpers for independent reference service."""
from __future__ import annotations
from contextlib import contextmanager
import sqlite3
import uuid
import re
import os
import stat
from pathlib import Path

def assert_canonical_table(conn, name: str, expected_sql: str) -> None:
    """Fail closed if an existing SQLite table no longer has the pinned DDL.

    This deliberately rejects unreviewed legacy/weak schemas: operators must
    perform a separately reviewed data migration rather than assume CREATE IF
    NOT EXISTS upgraded constraints. No schema is rewritten automatically.
    """
    row=conn.execute("SELECT type,sql FROM sqlite_master WHERE name=?",(name,)).fetchone()
    if row is None or row[0]!='table' or not isinstance(row[1],str):
        raise ValueError('Required state table missing or replaced: '+name)
    def canon(sql):
        return re.sub(r'\s+','',re.sub(r'\bIF\s+NOT\s+EXISTS\b','',sql,flags=re.I)).lower()
    if canon(row[1]) != canon(expected_sql):
        raise ValueError('Unsafe historical SQLite schema drift: '+name)

class SQLiteDatabase:
    """Per-operation connections; shared in-memory URI kept alive by an owner connection."""
    def __init__(self,path):
        # Bind a file-backed database to its original absolute name.
        # A later chdir must not silently redirect durable event/catalog state.
        self.path=str(path) if str(path)==':memory:' else str(Path(path).absolute())
        self._state_identity=None
        self._parent_identity=None
        self._keeper=None
        self._instance_id=None
        self.uri=False
        if self.path==':memory:':
            self.path='file:palate_'+uuid.uuid4().hex+'?mode=memory&cache=shared'
            self.uri=True
            self._keeper=sqlite3.connect(self.path,uri=True,timeout=15)
            self._keeper.execute('PRAGMA foreign_keys=ON')
        else:
            self._protect_state_path()
            with self.connect() as conn:
                conn.execute('PRAGMA journal_mode=WAL')
                # An internal persistent generation marker detects replacement
                # even when the OS reuses the old (device, inode) pair.
                conn.execute("""CREATE TABLE IF NOT EXISTS __palate_store_identity (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    generation TEXT NOT NULL CHECK(length(generation) = 32))""")
                conn.execute('INSERT OR IGNORE INTO __palate_store_identity VALUES (1,?)',
                             (uuid.uuid4().hex,))
                row=conn.execute('SELECT generation FROM __palate_store_identity WHERE singleton=1').fetchone()
                if not row or len(row[0])!=32:
                    raise ValueError('State database generation marker is corrupt')
                self._instance_id=row[0]
    def _protect_state_path(self):
        """Open a private file without following symlinks; never chmod shared parents.

        POSIX: directories we create are private (0700); DB inode is always 0600.
        Existing parents may be shared, but their DB cannot be read by other users.
        """
        p=Path(self.path)
        for parent in (p, *p.parents):
            if parent.is_symlink():
                raise ValueError('State path may not contain symlinks')
        parent=p.parent
        if not parent.exists():
            # pathlib mkdir(parents=True) applies default 0777 to intermediate
            # directories. Create every missing level with private 0700 instead.
            to_create=[]
            node=parent
            while not node.exists():
                to_create.append(node)
                node=node.parent
            if not node.is_dir() or node.is_symlink():raise ValueError('Unsafe state ancestor')
            for node in reversed(to_create):
                node.mkdir(mode=0o700)
        if not parent.is_dir():raise ValueError('State parent must be a directory')
        if os.name=='posix':
            parent_stat=parent.stat()
            # Holding the same pathname is not enough in a writable shared
            # directory: another local account could replace the DB or WAL.
            if parent_stat.st_uid!=os.getuid() or parent_stat.st_mode & (stat.S_IWGRP|stat.S_IWOTH):
                raise ValueError('State directory must be owned by the current user and not shared-writable')
            self._parent_identity=(parent_stat.st_dev,parent_stat.st_ino)
        if os.name=='posix':
            flags=os.O_CREAT|os.O_RDWR|getattr(os,'O_CLOEXEC',0)|getattr(os,'O_NOFOLLOW',0)
            fd=os.open(p,flags,0o600)
            try:
                if not stat.S_ISREG(os.fstat(fd).st_mode):
                    raise ValueError('State DB must be a regular file')
                os.fchmod(fd,0o600)
                current=os.fstat(fd)
                self._state_identity=(current.st_dev,current.st_ino)
            finally:
                os.close(fd)
        else:
            # On Windows, deployment must apply a private NTFS ACL to the directory.
            # Still refuse symlinks on both platforms.
            p.touch(exist_ok=True)
    def _verify_state_path(self):
        if self.uri:return
        p=Path(self.path)
        for segment in (p,*p.parents):
            if segment.is_symlink():raise ValueError('State path changed to a symlink')
        if os.name=='posix':
            parent=p.parent.stat()
            if (parent.st_dev,parent.st_ino)!=self._parent_identity or parent.st_uid!=os.getuid() or parent.st_mode & (stat.S_IWGRP|stat.S_IWOTH):
                raise ValueError('State directory replaced or made shared-writable')
            current=p.stat()
            if not stat.S_ISREG(current.st_mode) or (current.st_dev,current.st_ino)!=self._state_identity:
                raise ValueError('State database replaced after initialization')
            if current.st_uid != os.getuid() or stat.S_IMODE(current.st_mode) != 0o600:
                raise ValueError('State database permissions or ownership weakened')
            for suffix in ('-wal','-shm','-journal'):
                auxiliary=Path(self.path+suffix)
                if auxiliary.is_symlink():
                    raise ValueError('State database auxiliary path is a symlink')
                try: meta=auxiliary.stat()
                except FileNotFoundError:continue
                if (not stat.S_ISREG(meta.st_mode) or meta.st_uid != os.getuid() or
                    stat.S_IMODE(meta.st_mode) & 0o077):
                    raise ValueError('State database auxiliary permissions weakened')
        elif not p.is_file():
            raise ValueError('State database is not a regular file')
    @contextmanager
    def connect(self):
        self._verify_state_path()
        conn=sqlite3.connect(self.path,uri=self.uri,timeout=15)
        try:
            self._verify_state_path()
        except Exception:
            conn.close()
            raise
        try:
            if self._instance_id is not None:
                try:
                    row=conn.execute('SELECT generation FROM __palate_store_identity WHERE singleton=1').fetchone()
                    if not row or row[0]!=self._instance_id:
                        raise ValueError('State database generation changed after initialization')
                except sqlite3.DatabaseError as ex:
                    raise ValueError('State database generation marker unavailable') from ex
            conn.execute('PRAGMA foreign_keys=ON')
            conn.execute('PRAGMA busy_timeout=15000')
            with conn: yield conn
        finally: conn.close()
    def close(self):
        if self._keeper is not None:
            self._keeper.close()
            self._keeper=None
