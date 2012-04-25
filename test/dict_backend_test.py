from backend_test import BackendTest
from dict_backend import DictBackend
import unittest

class Single(BackendTest):
    def setUp(self):
        self.backend = DictBackend('test/data/test.dat')
        
    def tearDown(self):
        del self.backend
    
    def runTest(self):
        self.runSingleClient(self.backend)
        
# Run the tests.
if __name__ == '__main__':
    unittest.main()