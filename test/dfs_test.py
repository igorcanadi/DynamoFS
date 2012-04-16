import dynamo_fs
import server_stub

__author__ = 'jpaton'

import unittest

class BasicTest(unittest.TestCase):
    serverBackupFilename = 'test/data/server_stub_backup.dat'
    rootFilename = 'test/data/fs_root.txt'

    def setUp(self):
        self.ss = server_stub.ServerStub(MkdirLsTestCase.serverBackupFilename)
        self.dfs = dynamo_fs.DynamoFS(self.ss, MkdirLsTestCase.rootFilename)

    def tearDown(self):
        del self.dfs
        del self.ss

    def runTest(self):
        pass

class MkdirLsTestCase(BasicTest):
    def runTest(self):
        super(MkdirLsTestCase, self).runTest()
        self.dfs.mkdir('/', 'test_dir')
        self.dfs.mkdir('/', 'works')
        self.dfs.mkdir('/test_dir', 'win')
        self.dfs.mkdir('/test_dir/win', 'lose')
        self.assertEqual(self.dfs.ls('/'), ['test_dir', 'works'])
        self.assertEqual(self.dfs.ls('/'), ['test_dir', 'works'])
        self.assertEqual(self.dfs.ls('/test_dir'), ['win'])
        self.assertEqual(self.dfs.ls('/test_dir/win'), ['lose'])

class OpenWriteTestCase(MkdirLsTestCase):
    def runTest(self):
        super(OpenWriteTestCase, self).runTest()
        self.dfs.open("/test_dir/my_file", "w")