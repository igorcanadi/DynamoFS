import time
import config

class Controller:
    def __init__(self, server, root_filename, cache_size):
        self.server = server
        self.root_filename = root_filename
        self.cache = dict()
        self.cache_size = cache_size
        try:
            self.root_hash = open(root_filename, 'r').read().split('\n')[0]
        except IOError:
            self.root_hash = None

    def _relax_cache(self):
        if len(self.cache) >= float(self.cache_size) * config.ELASTIC_CACHE_OVERHEAD:
            entries_to_evict = len(self.cache) - self.cache_size 
            last_access_times = [(self.cache[x][0], x) for x in self.cache]
            last_access_times = sorted(last_access_times)[:entries_to_evict]
            for lat in last_access_times:
                del self.cache[lat[1]]

    def _add_to_cache(self, hash, value=None):
        self._relax_cache()
        t = time.time()
        if value != None: # forced
            self.cache[hash] = [t, value]
        elif hash not in self.cache: # don't have it in cache
            self.cache[hash] = [t, self.server.get(hash)]
        else: # just update access time
            self.cache[hash][0] = t

    def getdata(self, hash):
        self._add_to_cache(hash)
        if hash not in self.cache:
            exit(1)
        return self.cache[hash][1]

    def putdata(self, hash, value):
        self._add_to_cache(hash, value)
        self.server.put(hash, value)

    # return None if root hash not present
    def get_root_hash(self):
        return self.root_hash

    def update_root(self, new_root_hash):
        open(self.root_filename, 'w').write(new_root_hash)
