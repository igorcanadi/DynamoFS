from time import sleep
import benchmark_utils
from boto.sdb.connection import SDBConnection
import aws_credentials

TABLE_NAME = "data"

# We use single-character attribute names to save space in requests.
VALUE = "v"

# Get a feel for server-side latencies.
sampler = benchmark_utils.BenchmarkTimer()

# Backend driven by SimpleDB. We use one domain called "data".
##############################################################
# NOTE. We currently do not do reference-counting for SimpleDB. That is,
# nuke() is the only way that key-value pairs can be removed from the
# SimpleDBBackend. Reference-counting for SimpleDB would impose lots
# of extra overhead, since SimpleDB does not support atomic increments.
##############################################################
class SimpleDBBackend:
    
    def __init__(self):
        self.connection = SDBConnection(aws_credentials.accessKey,
                                        aws_credentials.secretKey)
        self.domain = self.connection.get_domain(TABLE_NAME)      


    def put(self, key, value):
        sampler.begin()
        try:
            self.domain.put_attributes(key, {VALUE:value})        
        finally:
            sampler.end()


    def get(self, key):
        sampler.begin()
        try:
            try:
                # First try an eventually consistent read.
                result = self.domain.get_attributes(key, consistent_read=False)
                return result[VALUE]
            except KeyError:
                # The eventually consistent read failed. Try a strongly consistent
                # read.
                result = self.domain.get_attributes(key, consistent_read=True)
                return result[VALUE]
        finally:
            sampler.end()
        

    def incRefCount(self, key):
        # Not implemented.
        pass
        
        
    def decRefCount(self, key):
        # Not implemented.
        pass
    
    
    def nuke(self):
        # Delete and re-create the table.
        self.connection.delete_domain(TABLE_NAME)
        self.domain = self.connection.create_domain(TABLE_NAME)
        
                    
    def flush(self):
        pass # No-op.
