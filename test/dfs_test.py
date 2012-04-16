import dynamo_fs
import server_stub

__author__ = 'jpaton'

import unittest

class MkdirLsTestCase(unittest.TestCase):
    def setUp(self):
        self.ss = server_stub.ServerStub('test/data/server_stub_backup.dat')
        self.dfs = dynamo_fs.DynamoFS(self.ss, 'test/data/fs_root.txt')

    def tearDown(self):
        del self.dfs
        del self.ss

    def runTest(self):
        self.dfs.mkdir('/', 'test_dir')
        self.dfs.mkdir('/', 'works')
        self.dfs.mkdir('/test_dir', 'win')
        self.assertEqual(self.dfs.ls('/'), ['test_dir', 'works'])
        self.assertEqual(self.dfs.ls('/'), ['test_dir', 'works'])
        self.assertEqual(self.dfs.ls('/test_dir'), ['win'])
