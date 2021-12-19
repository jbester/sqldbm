import unittest
import sqldbm
from sqldbm import Mode, SqliteDbm
import tempfile
import os
import shutil
import random
import shelve
from dataclasses import dataclass

@dataclass
class TestEntryData:
    id: int
    field1: str
    field2: str

class SqliteDbmTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.path = tempfile.mkdtemp()
        self.db_name = 'test.db'
        path = os.path.join(self.path, self.db_name)
        self.db = sqldbm.open(path, Mode.OPEN_CREATE_NEW)

    def tearDown(self) -> None:
        self.db.close()
        shutil.rmtree(self.path, True)

    def test_keys(self):
        """Verify keys are returned"""
        self.db['a'] = b'hello world'
        self.db['b'] = b'something else'
        self.assertEqual(set(['a', 'b']), set(self.db.keys()))

    def test_write(self):
        """Verify writes can be read back"""
        self.db['a'] = b'hello world'
        self.db['b'] = b'something else'
        self.assertEqual(b'hello world', self.db['a'])
        self.assertEqual(b'something else', self.db['b'])

    def test_replacement(self):
        """Verify writes can overwrite data"""
        self.db['a'] = b'hello world'
        self.assertEqual(b'hello world', self.db['a'])
        self.db['a'] = b'something else'
        self.assertEqual(b'something else', self.db['a'])

    def test_write_persistence(self):
        """Verify writes can be read after close"""
        self.db['a'] = b'hello world'
        self.db['b'] = b'something else'
        self.db.close()
        self.db = None
        self.db = sqldbm.open(os.path.join(self.path, self.db_name), Mode.OPEN)
        self.assertEqual(b'hello world', self.db['a'])
        self.assertEqual(b'something else', self.db['b'])

    def test_contains(self):
        """Verify contains detects if a record exists or not"""
        self.db['a'] = b'hello world'
        self.assertIn('a', self.db)
        self.assertNotIn('b', self.db)

    def test_sync(self):
        """Verify sync flushes to disk"""
        db_path = os.path.join(self.path, self.db_name)
        for i in range(10000):
            self.db[str(i)] = b'hello world'
        size_before = os.stat(db_path).st_size
        self.db.sync()
        size_after = os.stat(db_path).st_size
        self.assertTrue(size_before < size_after)

    def test_read_of_nonexistent_record(self):
        """Verify read of non-existent record returns None"""
        self.assertIsNone(self.db['a'])

    def test_force_create(self):
        """Verify force create removes exists file"""
        self.assertIsNone(self.db['a'])

    def test_len(self):
        """Verify len returns number of records"""
        count = random.randint(10000, 20000)
        for i in range(count):
            self.db[f"rec{i}"] = random.randbytes(5)
        self.assertEqual(count, len(self.db))

    def test_iter(self):
        """Verify iteration over database"""
        self.db['a'] = b'hello world'
        self.db['b'] = b'something else'
        key_set = set()
        for key in self.db:
            key_set.add(key)
        self.assertEqual(set(['a', 'b']), key_set)


class SqliteDbmUseCaseTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.path = tempfile.mkdtemp()
        self.db_name = 'test.db'
        pass

    def tearDown(self) -> None:
        shutil.rmtree(self.path, True)

    def test_use_case(self):
        """Verify general use case"""
        db_path = os.path.join(self.path, self.db_name)
        with sqldbm.open(db_path, Mode.OPEN_CREATE_NEW) as db:
            db['a'] = b'some data'
            self.assertEqual(db['a'], b'some data')
            db['a'] = b'some other data'
            self.assertEqual(db['a'], b'some other data')
            self.assertIn('a', db)
            del db['a']
            self.assertNotIn('a', db)

    def test_reopen(self):
        """Verify OPEN_CREATE does not erase existing file"""
        db_path = os.path.join(self.path, self.db_name)
        with sqldbm.open(db_path, Mode.OPEN_CREATE_NEW) as db:
            db['a'] = b'value'
        with sqldbm.open(db_path, Mode.OPEN_CREATE) as db:
            self.assertIn('a', db)
            self.assertEqual(db['a'], b'value')

    def test_interop_with_shelf(self):
        """Verify interop with shelf module"""
        db_path = os.path.join(self.path, self.db_name)
        entry1 = TestEntryData(1, 'entry1', 'entry2')
        entry2 = TestEntryData(2, 'hello', 'world')
        with sqldbm.open(db_path, Mode.OPEN_CREATE_NEW) as db:
            shelf = shelve.Shelf(db)
            shelf['key1'] = entry1
            shelf['key2'] = entry2
        with sqldbm.open(db_path, Mode.OPEN_CREATE) as db:
            with shelve.Shelf(db) as shelf:
                self.assertEqual(entry1, shelf['key1'])
                self.assertEqual(entry2, shelf['key2'])


if __name__ == '__main__':
    unittest.main()
