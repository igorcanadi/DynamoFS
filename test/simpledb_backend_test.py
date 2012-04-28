from backend_test import BackendTest
from simpledb_backend import SimpleDBBackend
import unittest

class Single(BackendTest):
    def setUp(self):
        self.backend = SimpleDBBackend()
        
    def tearDown(self):
        del self.backend
    
    def runTest(self):
        self.runSingleClient(self.backend, False)
        
class Multiple(BackendTest):
    def setUp(self):
        self.backend1 = SimpleDBBackend()
        self.backend2 = SimpleDBBackend()
        
    def tearDown(self):
        del self.backend1
        del self.backend2
    
    def runTest(self):
        self.runMultipleClients(self.backend1, self.backend2, False)
        
# Run the tests.
if __name__ == '__main__':
    unittest.main()