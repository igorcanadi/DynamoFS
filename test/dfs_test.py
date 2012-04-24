import dynamo_fs
import file
import os
import dict_backend 

__author__ = 'jpaton'

import unittest

class BasicTest(unittest.TestCase):
    serverBackupFilename = 'test/data/server_stub_backup.dat'
    rootFilename = 'test/data/fs_root.txt'

    def setUp(self):
        # we don't test persistence in these tests
        # start with clean plate
        self.ss = dict_backend.DictBackend(self.serverBackupFilename)
        self.ss.nuke()
        try:
          os.unlink(self.rootFilename)
        except:
          pass
        self.dfs = dynamo_fs.DynamoFS(self.ss, self.rootFilename)
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

class IsDirTestCase(BasicTest):
    def runTest(self):
        super(IsDirTestCase, self).runTest()
        self.dfs.mkdir('/', 'b')
        somefile = self.dfs.open('/b/a', "w")
        somefile.write('bla')
        somefile.close()
        self.assertEqual(self.dfs.isdir('/'), True)
        self.assertEqual(self.dfs.isdir('/b'), True)
        self.assertEqual(self.dfs.isdir('/b/a'), False)
        

class OpenReadTestCase(OpenWriteTestCase):
    def runTest(self):
        super(OpenReadTestCase, self).runTest()
        somefile = self.dfs.open(OpenWriteTestCase.filename, "r")
        self.assertEqual(OpenWriteTestCase.writtenString, somefile.read(100))
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

class SharingTestCase(BasicTest):
    def runTest(self):
        super(SharingTestCase, self).runTest()
        self.dfs.mkdir('/', 'movies')
        self.dfs.mkdir('/movies', 'romantic_comedies')
        f = self.dfs.open('/movies/to_watch', 'w')
        f.write('harry potter; star wars')
        f.close()
        k = self.dfs.get_key_for_sharing('/movies')
        self.dfs.attach_shared_key('/movies/', 'shared_movies', k)
        self.assertEqual(sorted(self.dfs.ls('/movies')), 
                sorted(['romantic_comedies', 'to_watch', 'shared_movies']))
        self.assertEqual(sorted(self.dfs.ls('/movies/shared_movies')), 
                sorted(['romantic_comedies', 'to_watch']))
        f1 = self.dfs.open('/movies/to_watch', 'r')
        f2 = self.dfs.open('/movies/shared_movies/to_watch', 'r')
        self.assertEqual(f1.read(100), f2.read(100))
        f1.close()
        f2.close()
        f2 = self.dfs.open('/movies/to_watch', 'w')
        f2.write('harry potter (DONE); star wars')
        f2.close()
        f1 = self.dfs.open('/movies/to_watch', 'r')
        f2 = self.dfs.open('/movies/shared_movies/to_watch', 'r')
        f1.close()
        f2.close()
        self.assertEqual(f1.read(100), 'harry potter (DONE); star wars')
        self.assertEqual(f2.read(100), 'harry potter; star wars')

class SimpleSeekTestCase(BasicTest):
    def runTest(self):
        super(SimpleSeekTestCase, self).runTest()
        somefile = self.dfs.open('/majmun', "w")
        somefile.write('ja sam mali majmun')
        somefile.close()
        somefile = self.dfs.open('/majmun', "r")
        self.assertEqual(somefile.read(3), 'ja ')
        self.assertEqual(somefile.read(3), 'sam')
        self.assertEqual(somefile.read(100), ' mali majmun')
        somefile.seek(2, file.SEEK_SET)
        self.assertEqual(somefile.read(6), ' sam m')
        somefile.seek(-2, file.SEEK_END)
        self.assertEqual(somefile.read(100), 'un')
        somefile.seek(0, file.SEEK_SET)
        self.assertEqual(somefile.read(3), 'ja ')
        somefile.seek(4, file.SEEK_CUR)
        self.assertEqual(somefile.read(11), 'mali majmun')

class BigSeekTestCase(BasicTest):
    def runTest(self):
        super(BigSeekTestCase, self).runTest()
        somefile = self.dfs.open('/veliki_majmun', "w")
        somefile.write(['a' if t < 4096 else 'b' for t in range(5500)])
        somefile.close()
        somefile = self.dfs.open('/veliki_majmun', "r")
        self.assertEqual(somefile.read(3000), "".join(['a' for i in range(3000)]))
        self.assertEqual(somefile.read(2000), "".join(['a' for i in range(1096)]) + "".join(['b' for i in range(904)]))
        somefile.seek(4, file.SEEK_SET)
        self.assertEqual(somefile.read(2), 'aa')
        somefile.seek(4095, file.SEEK_SET)
        self.assertEqual(somefile.read(2), 'ab')
        somefile.seek(4500, file.SEEK_SET)
        self.assertEqual(somefile.read(2), 'bb')
        somefile.seek(-4500, file.SEEK_END)
        self.assertEqual(somefile.read(2), 'aa')


if __name__ == '__main__':
    unittest.main()

