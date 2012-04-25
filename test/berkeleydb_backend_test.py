from backend_test import BackendTest
from berkeleydb_backend import BerkeleyDBBackend
import unittest

class Single(BackendTest):
    def setUp(self):
        self.backend = BerkeleyDBBackend('test/data/test.dat')
        
    def tearDown(self):
        del self.backend
    
    def runTest(self):
        self.runSingleClient(self.backend)
        
# We don't test BerkeleyDBBackend with multiple clients. We could probably get it
# to handle multiple clients, but we would need to use the more complicated PyBSDDB
# API.
        
# Run the tests.
if __name__ == '__main__':
    unittest.main()