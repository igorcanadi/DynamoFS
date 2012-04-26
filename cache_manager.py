import time
import config

class CacheManager:
    def __init__(self, cache_size):
        self.cache_size = cache_size
        self.cache = {}
        self.relaxing = False

    def _relax_cache(self):
        if self.relaxing:
            return
        self.relaxing = True
        if len(self.cache) >= float(self.cache_size) * config.ELASTIC_CACHE_OVERHEAD:
            entries_to_evict = len(self.cache) - self.cache_size 
            last_access_times = [(self.cache[x], x) for x in self.cache]
            last_access_times = sorted(last_access_times)[:entries_to_evict]
            for lat in last_access_times:
                if self.cache_size > len(self.cache):
                    break
                if lat[1] in self.cache:
                    # call the evict function
                    if lat[1]._key == None:
                        key = "None"
                    else:
                        key = lat[1]._key[:5]
#                    print "Cache evict: %s: %s %s" % (lat[1].__class__, lat[1].my_name(), key)
                    lat[1].evict()
        self.relaxing = False

    def add_to_cache(self, blob):
        if blob not in self.cache:
            have_to_relax = True
        else:
            have_to_relax = False
        self.cache[blob] = time.time()
        if have_to_relax:
#            print "Cache add: %s: %s %s" % (blob.__class__, blob.my_name(), blob._key[:5])
            self._relax_cache()

    def remove_from_cache(self, blob):
        if blob in self.cache:
            del self.cache[blob]

    def flush(self):
        # Evict all blobs from the cache.
        for blob in self.cache.keys():
            blob.evict()
            