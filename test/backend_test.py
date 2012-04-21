# This file is just a generic library for testing backends. It does not contain
# runnable testcases.

import unittest

# Helper class that contains generic code for testing any backend.
class BackendTest(unittest.TestCase):
    def runTestWithBackend(self, backend):
        # Create and garbage-collect one key.
        self.assertRaises(KeyError, backend.get, "key1")
        backend.put("key1", "value1")
        self.assertEqual(backend.get("key1"), "value1")
        backend.decRefCount("key1")
        self.assertRaises(KeyError, backend.get, "key1")
        
        # Create and garbage-collect multiple keys.
        backend.put("Seth", "Pollen")
        backend.put("Jim", "Paton")
        backend.put("Igor", "Canadi")
        self.assertEqual(backend.get("Igor"), "Canadi")
        self.assertEqual(backend.get("Jim"), "Paton")
        self.assertEqual(backend.get("Seth"), "Pollen")
        backend.incRefCount("Igor") # Preserve one key from GC for now.
        backend.decRefCount("Seth")
        backend.decRefCount("Jim")
        backend.decRefCount("Igor")
        self.assertEqual(backend.get("Igor"), "Canadi")
        self.assertRaises(KeyError, backend.get, "Jim")
        self.assertRaises(KeyError, backend.get, "Seth")
        backend.decRefCount("Igor") # Now garbage-collect Igor.
        self.assertRaises(KeyError, backend.get, "Igor")
        
        # Create some keys and then nuke them all.
        backend.put("Nikita", "Khruschev")
        backend.put("John", "Kennedy")
        backend.put("Fidel", "Castro")
        backend.nuke()
        self.assertRaises(KeyError, backend.get, "Fidel")
        self.assertRaises(KeyError, backend.get, "John")
        self.assertRaises(KeyError, backend.get, "Nikita")
        
        # Test out nuking again, to make sure it still works.
        backend.put("John", "Doe")
        self.assertEqual(backend.get("John"), "Doe")
        backend.nuke()
        self.assertRaises(KeyError, backend.get, "John")