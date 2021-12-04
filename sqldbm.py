import os
import sqlite3
import enum
import platform
from typing import Iterable, Optional


class Mode(enum.Enum):
    OPEN = 'rw'
    OPEN_CREATE = 'rwc'
    OPEN_CREATE_NEW = 'rwc'
    OPEN_READ_ONLY = 'ro'


class SqliteDbm:
    """Sqlite implementation of the DBM
    """

    def __init__(self, path, mode: str):
        """Constructor for a sqlite backed DBM implementation

        :param path: file path
        :param mode: sqlite3 open mode
        """
        if platform.system() == 'Windows':
            path = path.replace('\\', '/')
        self.db = sqlite3.connect(f'file:{path}?mode={mode}', uri=True)
        self.conn = self.db.cursor()
        self.conn.execute("CREATE TABLE IF NOT EXISTS Data (Key TEXT PRIMARY KEY UNIQUE NOT NULL, Value BLOB)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS data_key ON data(Key)")

    def __contains__(self, item: str) -> bool:
        """Test if a key exists within the database"""
        self.conn.execute("SELECT COUNT(*) FROM Data WHERE Key =  ?", (item,))
        count, = self.conn.fetchone()
        return count == 1

    def __len__(self) -> int:
        """Get the number of records in the database"""
        self.conn.execute("SELECT COUNT(*) FROM Data")
        count, = self.conn.fetchone()
        return count

    def __getitem__(self, item: str) -> Optional[bytes]:
        """Get the data associated with the given key or return None"""
        self.conn.execute("SELECT Value FROM data WHERE Key = ?", (item,))
        if v := self.conn.fetchone():
            return v[0]
        return None

    def __setitem__(self, key: str, value: bytes) -> None:
        """Insert or update the data in the database

        :param key: key
        :param value: data value
        """
        self.conn.execute("""INSERT INTO Data (Key, Value) VALUES (?, ?) 
                             ON CONFLICT(Key) DO UPDATE SET Value=excluded.Value""",
                          (key, value))

    def __delitem__(self, key: str):
        """Remove the key and record from the database"""
        self.conn.execute("DELETE FROM Data WHERE Key = ? ", (key,))

    def sync(self):
        """Write the database to disk"""
        self.db.commit()

    def close(self):
        """Write and close the database"""
        self.db.commit()
        self.db.close()
        self.conn = None
        self.db = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close upon exit"""
        if self.db is not None:
            self.close()

    def keys(self) -> Iterable[str]:
        """Get all keys in the database"""
        self.conn.execute("SELECT Key from Data")
        while r := self.conn.fetchone():
            yield r[0]


def open(path: str, mode: Mode) -> SqliteDbm:
    """Open a sqldbm database in the appropriate mode"""
    if mode == Mode.OPEN_CREATE_NEW:
        if os.path.exists(path):
            os.unlink(path)
    return SqliteDbm(path, mode.value)
