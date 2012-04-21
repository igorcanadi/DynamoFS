from backend_test import BackendTest
from dynamodb_backend import DynamoDBBackend
import unittest

class DynamoDBBackendTest(BackendTest):
    def setUp(self):
        self.backend = DynamoDBBackend()
        
    def tearDown(self):
        del self.backend
        
    def runTest(self):
        self.runTestWithBackend(self.backend)
        
# Run the tests.
if __name__ == '__main__':
    unittest.main()