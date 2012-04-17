from dict_backend import DictBackend
from sqlite_backend import SQLiteBackend

__author__ = 'sethpollen'

import unittest

# Helper class that contains generic code for testing any backend.
class BackendTest(unittest.TestCase):
    def runTestWithBackend(self, backend):
        # Create and garbage-collect one key.
        self.assertRaises(KeyError, backend.get, "key1")
        backend.put("key1", "value1")
        self.assertEqual(backend.get("key1"), "value1")
        backend.decRefCount("key1")
        self.assertRaises(KeyError, backend.get, "key1")
        
        # Create and garbage-collect multiple keys.
        backend.put("Seth", "Pollen")
        backend.put("Jim", "Paton")
        backend.put("Igor", "Canadi")
        self.assertEqual(backend.get("Igor"), "Canadi")
        self.assertEqual(backend.get("Jim"), "Paton")
        self.assertEqual(backend.get("Seth"), "Pollen")
        backend.incRefCount("Igor") # Preserve one key from GC for now.
        backend.decRefCount("Seth")
        backend.decRefCount("Jim")
        backend.decRefCount("Igor")
        self.assertEqual(backend.get("Igor"), "Canadi")
        self.assertRaises(KeyError, backend.get, "Jim")
        self.assertRaises(KeyError, backend.get, "Seth")
        backend.decRefCount("Igor") # Now garbage-collect Igor.
        self.assertRaises(KeyError, backend.get, "Igor")

class SQLiteBackendTest(BackendTest):
    def setUp(self):
        self.backend = SQLiteBackend('test/data/test.db')
        
    def tearDown(self):
        del self.backend
        
    def runTest(self):
        self.runTestWithBackend(self.backend)
        
class DictBackendTest(BackendTest):
    def setUp(self):
        self.backend = DictBackend('test/data/test.dat')
        
    def tearDown(self):
        del self.backend
        
    def runTest(self):
        self.runTestWithBackend(self.backend)
