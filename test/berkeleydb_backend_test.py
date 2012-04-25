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
        
class Multiple(BackendTest):
    def setUp(self):
        self.backend1 = BerkeleyDBBackend('test/data/test.dat')
        self.backend2 = BerkeleyDBBackend('test/data/test.dat')
        
    def tearDown(self):
        del self.backend1
        del self.backend2
    
    def runTest(self):
        self.runMultipleClients(self.backend1, self.backend2, False)
        
# Run the tests.
if __name__ == '__main__':
    unittest.main()