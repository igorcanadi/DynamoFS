import time
import config

class CacheManager:
    def __init__(self, cache_size):
        self.cache_size = cache_size
        self.cache = {}

    def _relax_cache(self):
        if len(self.cache) >= float(self.cache_size) * config.ELASTIC_CACHE_OVERHEAD:
            entries_to_evict = len(self.cache) - self.cache_size 
            last_access_times = [(self.cache[x], x) for x in self.cache]
            last_access_times = sorted(last_access_times)[:entries_to_evict]
            for lat in last_access_times:
                if self.cache_size > len(self.cache):
                    break
                if lat[1] in self.cache:
                    # call the evict function
                    lat[1]()

    def add_to_cache(self, evict_function):
        self.cache[evict_function] = time.time()
        self._relax_cache()

    def remove_from_cache(self, evict_function):
        del self.cache[evict_function]

