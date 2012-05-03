import dynamo_fs
import file
import os
import dict_backend 
import unittest

class BasicTest(unittest.TestCase):
    serverBackupFilename = 'test/data/server_stub_backup.dat'
    rootFilename = 'test/data/fs_root.txt'

    def create_tree(self, path, structure):
        if isinstance(structure, dict):
            for f in structure:
                if isinstance(structure[f], dict):
                    self.dfs.mkdir(path, f)
                self.create_tree(path + '/' + f, structure[f])
        elif isinstance(structure, str):
            f = self.dfs.open(path, 'w')
            f.write(structure)
            f.close()

    def check_structure(self, blob, structure):
        if isinstance(structure, dict):
            for f in structure:
                self.check_structure(blob[f], structure[f])
            for c in blob.keys():
                self.assertTrue(c in structure)
        elif isinstance(structure, str):
            f = file.File(blob, 'r')
            self.assertEqual(f.read(10000), structure)

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
        print "Final state (" + self.__class__.__name__ + ")"
        self.dfs.debug_output_whole_tree()
        del self.dfs
        del self.ss

    def runTest(self):
        pass

class MkdirLsTestCase(BasicTest):
    def runTest(self):
        super(MkdirLsTestCase, self).runTest()
        self.create_tree('/', {'test_dir': {'win': {'lose': {}} }, 'works': {}})
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
        self.create_tree('/', {'a': {'test_dir': {}, 'works': {}}})
        self.check_structure(self.dfs.root, {'a': {'test_dir': {}, 'works': {}}})
        self.dfs.rm('/a/test_dir')
        self.check_structure(self.dfs.root, {'a': {'works': {}}})

class MvTestCase(BasicTest):
    def runTest(self):
        super(MvTestCase, self).runTest()
        self.create_tree('/', {'a': {'test_dir': {}, 'works': {}}, 'b': {}})
        self.check_structure(self.dfs.root, {'a': {'test_dir': {}, 'works': {}}, 'b': {}})
        self.dfs.mv('/a/test_dir', '/a/pas')
        self.check_structure(self.dfs.root, {'a': {'pas': {}, 'works': {}}, 'b': {}})
        self.dfs.mv('/a/pas', '/b/macka')
        self.check_structure(self.dfs.root, {'a': {'works': {}}, 'b': {'macka': {}}})

class SharingTestCase(BasicTest):
    def runTest(self):
        super(SharingTestCase, self).runTest()
        self.create_tree('/', 
                {'movies': {
                    'romantic_comedies': {}, 
                    'to_watch': 'harry potter; star wars'
                    }
                }
        )
        k = self.dfs.get_key_for_sharing('/movies')
        self.dfs.attach_shared_key('/movies/', 'shared_movies', k)

        self.check_structure(self.dfs.root,
                {'movies': {
                    'romantic_comedies': {}, 
                    'to_watch': 'harry potter; star wars',
                    'shared_movies': {
                        'romantic_comedies': {}, 
                        'to_watch': 'harry potter; star wars'
                        }
                    }
                }
        )

        f2 = self.dfs.open('/movies/to_watch', 'w')
        f2.write('harry potter (DONE); star wars')
        f2.close()
        self.check_structure(self.dfs.root,
                {'movies': {
                    'romantic_comedies': {}, 
                    'to_watch': 'harry potter (DONE); star wars',
                    'shared_movies': {
                        'romantic_comedies': {}, 
                        'to_watch': 'harry potter; star wars'
                        }
                    }
                }
        )
        
class FlushTestCase(BasicTest):
    def runTest(self):
        super(FlushTestCase, self).runTest()
        
        # Based on SharingTestCase, just with a bunch of flush() calls in
        # important places.
        
        self.create_tree('/', 
                {'movies': {
                    'romantic_comedies': {}, 
                    'to_watch': 'harry potter; star wars'
                    }
                }
        )
        
        self.dfs.flush()
        
        k = self.dfs.get_key_for_sharing('/movies')
        
        self.dfs.flush()
        
        self.dfs.attach_shared_key('/movies/', 'shared_movies', k)
        
        self.dfs.flush()
        
        self.check_structure(self.dfs.root,
                {'movies': {
                    'romantic_comedies': {}, 
                    'to_watch': 'harry potter; star wars',
                    'shared_movies': {
                        'romantic_comedies': {}, 
                        'to_watch': 'harry potter; star wars'
                        }
                    }
                }
        )
        
        f2 = self.dfs.open('/movies/to_watch', 'w')
        f2.write('harry potter (DONE); star wars')
        f2.close()
        
        f1 = self.dfs.open('/movies/to_watch', 'r')
        
        self.dfs.flush()
        
        f2 = self.dfs.open('/movies/shared_movies/to_watch', 'r')
        
        self.assertEqual(f1.read(100), 'harry potter (DONE); star wars')
        self.assertEqual(f2.read(100), 'harry potter; star wars')
        
        f1.close()
        f2.close()

class MergingTestCase(BasicTest):
    # /movies
    #     /batman
    #          /bill
    #             note -> 'foo'
    #          /rose
    #     /spiderman
    #         /star_wars
    #              lyrics -> 'la la la'
    # /movies_to_be_merged
    #     /batman
    #         /bill
    #             note -> 'old_foo'
    #             /bong
    #               note -> 'yeah'
    #         /bruce
    #            talk -> 'tt'
    #     /spiderman
    #         /star_wars
    #             /lyrics
    #                lyrics -> 'la la'
    #    /harry_potter
    #          /chamber

    def runTest(self):
        super(MergingTestCase, self).runTest()
        self.create_tree('/', \
            {
                'movies': {
                    'batman': {'bill': {'note': 'foo'}, 'rose': {}},
                    'spiderman': {'star_wars': {'lyrics': 'la la la'}}
                },
                'movies_to_be_merged': {
                    'batman': {
                        'bill': {'bong': {'note': 'yeah'}, 'note': 'old_foo'},
                        'bruce': {'talk': 'tt'}
                    },
                    'spiderman': {
                        'star_wars': {'lyrics': {'lyrics': 'la la'}}
                    },
                    'harry_potter': {
                        'chamber': {}
                    }
                }
            }
        )
        k = self.dfs.get_key_for_sharing('/movies_to_be_merged')
        self.dfs.merge_with_shared_key('/movies', k)
        self.check_structure(self.dfs.root['movies'], \
            {
                'batman': {
                    'bruce': {'talk': 'tt'},
                    'bill': {'note': 'old_foo', 'bong': {'note': 'yeah'}},
                    'rose': {}
                },
                'spiderman': {'star_wars': {'lyrics': {'lyrics': 'la la'}}},
                'harry_potter': { 'chamber': {} }
            }
        )


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

class PersistenceTestCase(BasicTest):
    def runTest(self):
        super(PersistenceTestCase, self).runTest()
        self.create_tree('/', \
            {
                'movies': {
                    'batman': {'bill': {'note': 'foo'}, 'rose': {}},
                    'spiderman': {'star_wars': {'lyrics': 'la la la'}}
                },
                'movies_to_be_merged': {
                    'batman': {
                        'bill': {'bong': {'note': 'yeah'}, 'note': 'old_foo'},
                        'bruce': {'talk': 'tt'}
                    },
                    'spiderman': {
                        'star_wars': {'lyrics': {'lyrics': 'la la'}}
                    },
                    'harry_potter': {
                        'chamber': {}
                    }
                }
            }
        )
        self.dfs.flush()
        local_ss = dict_backend.DictBackend(self.serverBackupFilename)
        local_dfs = dynamo_fs.DynamoFS(local_ss, self.rootFilename)
        self.check_structure(local_dfs.root, \
            {
                'movies': {
                    'batman': {'bill': {'note': 'foo'}, 'rose': {}},
                    'spiderman': {'star_wars': {'lyrics': 'la la la'}}
                },
                'movies_to_be_merged': {
                    'batman': {
                        'bill': {'bong': {'note': 'yeah'}, 'note': 'old_foo'},
                        'bruce': {'talk': 'tt'}
                    },
                    'spiderman': {
                        'star_wars': {'lyrics': {'lyrics': 'la la'}}
                    },
                    'harry_potter': {
                        'chamber': {}
                    }
                }
            }
        )





if __name__ == '__main__':
    unittest.main()

