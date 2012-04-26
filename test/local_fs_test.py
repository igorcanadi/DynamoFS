import shutil
import unittest
import local_fs
import file

root = 'test/data/localfs'

class BasicTest(unittest.TestCase):
    def setUp(self):
        try:
            shutil.rmtree(root)
        except:
            pass
        self.fs = local_fs.LocalFS(root)

    def tearDown(self):
        del self.fs

    def runTest(self):
        pass

class MkdirLsTestCase(BasicTest):
    def runTest(self):
        super(MkdirLsTestCase, self).runTest()
        self.fs.mkdir('/', 'test_dir')
        self.fs.mkdir('/', 'works')
        self.fs.mkdir('/test_dir', 'win')
        self.fs.mkdir('/test_dir/win', 'lose')
        self.assertEqual(self.fs.ls('/'), ['test_dir', 'works'])
        self.assertEqual(self.fs.ls('/'), ['test_dir', 'works'])
        self.assertEqual(self.fs.ls('/test_dir'), ['win'])
        self.assertEqual(self.fs.ls('/test_dir/win'), ['lose'])

class OpenWriteTestCase(MkdirLsTestCase):
    writtenString = "Hello, world!"
    filename = "/test_dir/my_file"
    def runTest(self):
        super(OpenWriteTestCase, self).runTest()
        somefile = self.fs.open(OpenWriteTestCase.filename, "w")
        somefile.write(OpenWriteTestCase.writtenString)
        somefile.close()

class IsDirTestCase(BasicTest):
    def runTest(self):
        super(IsDirTestCase, self).runTest()
        self.fs.mkdir('/', 'b')
        somefile = self.fs.open('/b/a', "w")
        somefile.write('bla')
        somefile.close()
        self.assertEqual(self.fs.isdir('/'), True)
        self.assertEqual(self.fs.isdir('/b'), True)
        self.assertEqual(self.fs.isdir('/b/a'), False)
        
class OpenReadTestCase(OpenWriteTestCase):
    def runTest(self):
        super(OpenReadTestCase, self).runTest()
        somefile = self.fs.open(OpenWriteTestCase.filename, "r")
        self.assertEqual(OpenWriteTestCase.writtenString, somefile.read(100))
        somefile.close()

class RmTestCase(BasicTest):
    def runTest(self):
        super(RmTestCase, self).runTest()
        self.fs.mkdir('/', 'a')
        self.fs.mkdir('/a', 'test_dir')
        self.fs.mkdir('/a', 'works')
        self.assertEqual(self.fs.ls('/a'), ['test_dir', 'works'])
        self.fs.rm('/a/test_dir')
        self.assertEqual(self.fs.ls('/a'), ['works'])

class MvTestCase(BasicTest):
    def runTest(self):
        super(MvTestCase, self).runTest()
        self.fs.mkdir('/', 'a')
        self.fs.mkdir('/', 'b')
        self.fs.mkdir('/a', 'test_dir')
        self.fs.mkdir('/a', 'works')
        self.assertEqual(self.fs.ls('/a'), ['test_dir', 'works'])
        self.fs.mv('/a/test_dir', '/a/pas')
        self.assertEqual(self.fs.ls('/a'), ['works', 'pas'])
        self.fs.mv('/a/pas', '/b/macka')
        self.assertEqual(self.fs.ls('/b'), ['macka'])
        self.assertEqual(self.fs.ls('/a'), ['works'])

class SimpleSeekTestCase(BasicTest):
    def runTest(self):
        super(SimpleSeekTestCase, self).runTest()
        somefile = self.fs.open('/majmun', "w")
        somefile.write('ja sam mali majmun')
        somefile.close()
        somefile = self.fs.open('/majmun', "r")
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
        somefile = self.fs.open('/veliki_majmun', "w")
        somefile.write("".join(['a' if t < 4096 else 'b' for t in range(5500)]))
        somefile.close()
        somefile = self.fs.open('/veliki_majmun', "r")
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

