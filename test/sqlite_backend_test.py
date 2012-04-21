from backend_test import BackendTest
from sqlite_backend import SQLiteBackend
import unittest

class SQLiteBackendTest(BackendTest):
    def setUp(self):
        self.backend = SQLiteBackend('test/data/test.db')
        
    def tearDown(self):
        del self.backend
        
    def runTest(self):
        self.runTestWithBackend(self.backend)
        
# Run the tests.
if __name__ == '__main__':
    unittest.main()