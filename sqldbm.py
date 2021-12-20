import os
import sqlite3
import enum
import platform
from typing import Iterable, Optional, List
from collections.abc import MutableMapping
from contextlib import contextmanager


@contextmanager
def cursor(db):
    cur = db.cursor()
    try:
        yield cur
    finally:
        cur.close()


class Mode(enum.Enum):
    """File open mode

     - OPEN - open a file for read/write
     - OPEN_CREATE - open a file, create if it doesn't exist
     - OPEN_CREATE_NEW - open a file, force creation even if it exists
     - OPEN_READ_ONLY - open a file for read-only purposes
    """
    OPEN = 'rw'
    OPEN_CREATE = 'rwc'
    OPEN_CREATE_NEW = 'rwcn'
    OPEN_READ_ONLY = 'ro'


class SqliteDbmTable(MutableMapping):
    """Sqlite implementation of the DBM
    """

    def __init__(self, db: sqlite3.Connection, table_name: str):
        """Constructor for a sqlite backed DBM implementation

        :param db: db file path
        :param table_name: table name to use
        """
        self.db = db
        self.table_name = table_name
        with cursor(self.db) as cur:
            cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (Key TEXT PRIMARY KEY UNIQUE NOT NULL, Value BLOB)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS {table_name}_key ON {table_name}(Key)")

    def __contains__(self, item: str) -> bool:
        """Test if a key exists within the database"""
        with cursor(self.db) as cur:
            cur.execute(f"SELECT COUNT(*) FROM {self.table_name} WHERE Key =  ?", (item,))
            count, = cur.fetchone()
        return count == 1

    def __len__(self) -> int:
        """Get the number of records in the database"""
        with cursor(self.db) as cur:
            cur.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            count, = cur.fetchone()
        return count

    def __getitem__(self, item: str) -> Optional[bytes]:
        """Get the data associated with the given key or return None"""
        with cursor(self.db) as cur:
            cur.execute(f"SELECT Value FROM {self.table_name} WHERE Key = ?", (item,))
            if v := cur.fetchone():
                return v[0]
            return None

    def __setitem__(self, key: str, value: bytes) -> None:
        """Insert or update the data in the database

        :param key: key
        :param value: data value
        """
        with cursor(self.db) as cur:
            cur.execute(f"""INSERT INTO {self.table_name} (Key, Value) VALUES (?, ?) 
                                ON CONFLICT(Key) DO UPDATE SET Value=excluded.Value""",
                            (key, value))

    def __delitem__(self, key: str):
        """Remove the key and record from the database"""
        with cursor(self.db) as cur:
            cur.execute(f"DELETE FROM {self.table_name} WHERE Key = ? ", (key,))

    def __iter__(self) -> Iterable[str]:
        """Get all keys in the database"""
        with cursor(self.db) as cur:
            cur.execute(f"SELECT Key FROM {self.table_name}")
            while r := cur.fetchone():
                yield r[0]

    def keys(self) -> List[str]:
        """Get all keys in the database"""
        return list(k for k in self)


class SqliteDbm:
    """Sqlite implementation of the DBM
    """

    def __init__(self, db_path: str, mode: str):
        """Constructor for a sqlite backed DBM implementation

        :param db_path: db file path
        :param mode: sqlite3 open mode
        """
        if platform.system() == 'Windows':
            db_path = db_path.replace('\\', '/')
        self.db = sqlite3.connect(f'file:{db_path}?mode={mode}', uri=True)
        self.tables = {}

    def __getitem__(self, table_name):
        """Get the table name"""
        if table_name in self.tables:
            return self.tables[table_name]
        return SqliteDbmTable(self.db, table_name)

    def sync(self):
        """Write the database to disk"""
        if self.db is not None:
            self.db.commit()

    def close(self):
        """Write and close the database"""
        if self.db is not None:
            self.db.commit()
            self.db.close()
        self.db = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close upon exit"""
        if self.db is not None:
            self.close()


def open(path: str, mode: Mode) -> SqliteDbm:
    """Open a sqldbm database in the appropriate mode"""
    if mode == Mode.OPEN_CREATE_NEW:
        if os.path.exists(path):
            os.unlink(path)
        mode = Mode.OPEN_CREATE
    return SqliteDbm(path, mode.value)

