from backend_test import BackendTest
from s3_backend import S3Backend
import unittest

class Single(BackendTest):
    def setUp(self):
        self.backend = S3Backend()
        
    def tearDown(self):
        del self.backend
    
    def runTest(self):
        self.runSingleClient(self.backend, False)
        
class Multiple(BackendTest):
    def setUp(self):
        self.backend1 = S3Backend()
        self.backend2 = S3Backend()
        
    def tearDown(self):
        del self.backend1
        del self.backend2
    
    def runTest(self):
        self.runMultipleClients(self.backend1, self.backend2, False)
        
# Run the tests.
if __name__ == '__main__':
    unittest.main()