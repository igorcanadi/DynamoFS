import dynamo_fs
import dict_backend 

__author__ = 'jpaton'

import unittest

class BasicTest(unittest.TestCase):
    serverBackupFilename = 'test/data/server_stub_backup.dat'
    rootFilename = 'test/data/fs_root.txt'

    def setUp(self):
        self.ss = dict_backend.DictBackend(MkdirLsTestCase.serverBackupFilename)
        self.dfs = dynamo_fs.DynamoFS(self.ss, MkdirLsTestCase.rootFilename)
        print "Initial state"
        self.dfs.debug_output_whole_tree()

    def tearDown(self):
        print "Final state"
        self.dfs.debug_output_whole_tree()
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
    writtenString = "Hello, world!"
    filename = "/test_dir/my_file"
    def runTest(self):
        super(OpenWriteTestCase, self).runTest()
        somefile = self.dfs.open(OpenWriteTestCase.filename, "w")
        somefile.write(OpenWriteTestCase.writtenString)
        somefile.close()

class OpenReadTestCase(OpenWriteTestCase):
    def runTest(self):
        super(OpenReadTestCase, self).runTest()
        somefile = self.dfs.open(OpenWriteTestCase.filename, "r")
        self.assertEqual(OpenWriteTestCase.writtenString, somefile.read())
        somefile.close()

class RmTestCase(BasicTest):
    def runTest(self):
        super(RmTestCase, self).runTest()
        self.dfs.mkdir('/', 'a')
        self.dfs.mkdir('/a', 'test_dir')
        self.dfs.mkdir('/a', 'works')
        self.assertEqual(self.dfs.ls('/a'), ['test_dir', 'works'])
        self.dfs.rm('/a/test_dir')
        self.assertEqual(self.dfs.ls('/a'), ['works'])

class MvTestCase(BasicTest):
    def runTest(self):
        super(MvTestCase, self).runTest()
        self.dfs.mkdir('/', 'a')
        self.dfs.mkdir('/a', 'test_dir')
        self.dfs.mkdir('/a', 'works')
        self.assertEqual(self.dfs.ls('/a'), ['test_dir', 'works'])
        self.dfs.mv('/a/', 'test_dir', 'pas')
        self.assertEqual(self.dfs.ls('/a'), ['works', 'pas'])
