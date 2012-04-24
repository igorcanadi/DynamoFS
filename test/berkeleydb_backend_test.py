from backend_test import BackendTest
from berkeleydb_backend import BerkeleyDBBackend
import unittest

class DictBackendTest(BackendTest):
    def setUp(self):
        self.backend = BerkeleyDBBackend('test/data/test.dat')
        
    def tearDown(self):
        del self.backend
        
    def runTest(self):
        self.runTestWithBackend(self.backend)
        
# Run the tests.
if __name__ == '__main__':
    unittest.main()