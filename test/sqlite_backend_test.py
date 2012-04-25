from backend_test import BackendTest
from sqlite_backend import SQLiteBackend
import unittest

class Single(BackendTest):
    def setUp(self):
        self.backend = SQLiteBackend('test/data/test.dat')
        
    def tearDown(self):
        del self.backend
    
    def runTest(self):
        self.runSingleClient(self.backend)
        
class Multiple(BackendTest):
    def setUp(self):
        self.backend1 = SQLiteBackend('test/data/test.dat')
        self.backend2 = SQLiteBackend('test/data/test.dat')
        
    def tearDown(self):
        del self.backend1
        del self.backend2
    
    def runTest(self):
        self.runMultipleClients(self.backend1, self.backend2)
        
# Run the tests.
if __name__ == '__main__':
    unittest.main()