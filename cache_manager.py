import time

ELASTIC_CACHE_BUFFER = 1000

class CacheManager:
    def __init__(self, cache_size):
        self.cache_size = cache_size
        self.cache = {}

    def _relax_cache(self):
        if len(self.cache) >= self.cache_size + ELASTIC_CACHE_BUFFER:
            entries_to_evict = len(self.cache) - self.cache_size 
            last_access_times = [(self.cache[x], x) for x in self.cache]
            last_access_times = sorted(last_access_times)[:entries_to_evict]
            for lat in last_access_times:
                del self.cache[lat[1]]
                lat[1]() # call the evict function
    
    def add_to_cache(self, evict_function):
        self.cache[evict_function] = time.time()
        self._relax_cache()

