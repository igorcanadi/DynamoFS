# This file is just a generic library for testing backends. It does not contain
# runnable testcases.

import unittest

# Helper class that contains generic code for testing any backend.
class BackendTest(unittest.TestCase):
    # Tests for a single backend.
    # testDeletes - set to false if you don't want to require deletes to be
    #               eagerly propagated.
    def runSingleClient(self, backend, testDeletes=True):
        backend.nuke()
        
        # Create and garbage-collect one key.
        self.assertRaises(KeyError, backend.get, "key1")
        backend.put("key1", "value1")
        self.assertEqual(backend.get("key1"), "value1")
        backend.decRefCount("key1")
        if testDeletes:
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
        if testDeletes:
            self.assertRaises(KeyError, backend.get, "Jim")
            self.assertRaises(KeyError, backend.get, "Seth")
        backend.decRefCount("Igor") # Now garbage-collect Igor.
        if testDeletes:
            self.assertRaises(KeyError, backend.get, "Igor")
        
        # Create some keys and then nuke them all.
        backend.put("Nikita", "Khruschev")
        backend.put("John", "Kennedy")
        backend.put("Fidel", "Castro")
        backend.nuke()
        self.assertRaises(KeyError, backend.get, "Fidel")
        self.assertRaises(KeyError, backend.get, "John")
        self.assertRaises(KeyError, backend.get, "Nikita")
    
    # Tests for consistency between two instances of a backend.
    # testDeletes - set to false if you don't want to require deletes to be
    #               eagerly propagated.
    def runMultipleClients(self, a, b, testDeletes=True):
        self.createMessage(a, b, 1)
        if testDeletes:
            self.deleteMessage(a, b)
        
        self.createMessage(b, a, 2)
        if testDeletes:
            self.deleteMessage(b, a)
        
        self.createMessage(a, b, 3)
        if testDeletes:
            self.deleteMessage(b, a)
        
        self.createMessage(b, a, 4)
        if testDeletes:
            self.deleteMessage(a, b)
        
    # Sends a key from one backend to another.
    def createMessage(self, b1, b2, i):
        key = "m" + str(i)
        value = "v" + str(i)
        b1.put(key, value)
        self.assertEqual(b2.get(key), value)
        
    # Deletes a key from one backend to another.
    def deleteMessage(self, b1, b2, i):
        key = "m" + str(i)
        b1.decRefCount(key)
        self.assertRaises(KeyError, b2.get, key)
        