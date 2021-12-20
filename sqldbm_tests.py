import unittest
import sqldbm
from sqldbm import Mode
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
        self.data_table = self.db['data']

    def tearDown(self) -> None:
        self.db.close()
        shutil.rmtree(self.path, True)

    def test_keys(self):
        """Verify keys are returned"""
        self.data_table['a'] = b'hello world'
        self.data_table['b'] = b'something else'
        self.assertEqual(set(['a', 'b']), set(self.data_table.keys()))

    def test_write(self):
        """Verify writes can be read back"""
        self.data_table['a'] = b'hello world'
        self.data_table['b'] = b'something else'
        self.assertEqual(b'hello world', self.data_table['a'])
        self.assertEqual(b'something else', self.data_table['b'])

    def test_replacement(self):
        """Verify writes can overwrite data"""
        self.data_table['a'] = b'hello world'
        self.assertEqual(b'hello world', self.data_table['a'])
        self.data_table['a'] = b'something else'
        self.assertEqual(b'something else', self.data_table['a'])

    def test_write_persistence(self):
        """Verify writes can be read after close"""
        self.data_table['a'] = b'hello world'
        self.data_table['b'] = b'something else'
        self.db.close()
        self.db = sqldbm.open(os.path.join(self.path, self.db_name), Mode.OPEN)
        data_table = self.db['data']
        self.assertEqual(b'hello world', data_table['a'])
        self.assertEqual(b'something else', data_table['b'])

    def test_contains(self):
        """Verify contains detects if a record exists or not"""
        self.data_table['a'] = b'hello world'
        self.assertIn('a', self.data_table)
        self.assertNotIn('b', self.data_table)

    def test_sync(self):
        """Verify sync flushes to disk"""
        db_path = os.path.join(self.path, self.db_name)
        for i in range(10000):
            self.data_table[str(i)] = b'hello world'
        size_before = os.stat(db_path).st_size
        self.db.sync()
        size_after = os.stat(db_path).st_size
        self.assertTrue(size_before < size_after)

    def test_read_of_nonexistent_record(self):
        """Verify read of non-existent record returns None"""
        self.assertIsNone(self.data_table['a'])

    def test_force_create(self):
        """Verify force create removes exists file"""
        self.assertIsNone(self.data_table['a'])

    def test_len(self):
        """Verify len returns number of records"""
        count = random.randint(10000, 20000)
        for i in range(count):
            self.data_table[f"rec{i}"] = random.randbytes(5)
        self.assertEqual(count, len(self.data_table))

    def test_iter(self):
        """Verify iteration over database"""
        self.data_table['a'] = b'hello world'
        self.data_table['b'] = b'something else'
        key_set = set()
        for key in self.data_table:
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
            data_table = db['data']
            data_table['a'] = b'some data'
            self.assertEqual(data_table['a'], b'some data')
            data_table['a'] = b'some other data'
            self.assertEqual(data_table['a'], b'some other data')
            self.assertIn('a', data_table)
            del data_table['a']
            self.assertNotIn('a', data_table)

    def test_reopen(self):
        """Verify OPEN_CREATE does not erase existing file"""
        db_path = os.path.join(self.path, self.db_name)
        with sqldbm.open(db_path, Mode.OPEN_CREATE_NEW) as db:
            data_table = db['data']
            data_table['a'] = b'value'
        with sqldbm.open(db_path, Mode.OPEN_CREATE) as db:
            data_table = db['data']
            self.assertIn('a', data_table)
            self.assertEqual(data_table['a'], b'value')

    def test_interop_with_shelf(self):
        """Verify interop with shelf module"""
        db_path = os.path.join(self.path, self.db_name)
        entry1 = TestEntryData(1, 'entry1', 'entry2')
        entry2 = TestEntryData(2, 'hello', 'world')
        with sqldbm.open(db_path, Mode.OPEN_CREATE_NEW) as db:
            data_table = db['data']
            shelf = shelve.Shelf(data_table)
            shelf['key1'] = entry1
            shelf['key2'] = entry2
        with sqldbm.open(db_path, Mode.OPEN_CREATE) as db:
            data_table = db['data']
            with shelve.Shelf(data_table) as shelf:
                self.assertEqual(entry1, shelf['key1'])
                self.assertEqual(entry2, shelf['key2'])

    def test_multiple_tables(self):
        """Verify support for multiple concurrent tables"""
        db_path = os.path.join(self.path, self.db_name)
        entry1 = TestEntryData(1, 'entry1', 'entry2')
        entry2 = TestEntryData(2, 'hello', 'world')
        entry3 = TestEntryData(3, 'another', 'entry')
        entry4 = TestEntryData(4, 'some other', 'data')
        with sqldbm.open(db_path, Mode.OPEN_CREATE) as db:
            table1 = db['table1']
            table2 = db['table2']
            # enter data with overlapping keys
            with shelve.Shelf(table1) as shelf1:
                shelf2 = shelve.Shelf(table2)
                shelf1['key2'] = entry2
                shelf2['key2'] = entry3
                shelf2['key3'] = entry4
                shelf1['key1'] = entry1
        with sqldbm.open(db_path, Mode.OPEN) as db:
            # verify key/values are in the appropriate shelf
            table1 = db['table1']
            table2 = db['table2']
            shelf1 = shelve.Shelf(table1)
            shelf2 = shelve.Shelf(table2)
            self.assertIn('key1', shelf1)
            self.assertIn('key2', shelf1)
            self.assertIn('key2', shelf2)
            self.assertIn('key3', shelf2)


if __name__ == '__main__':
    unittest.main()
