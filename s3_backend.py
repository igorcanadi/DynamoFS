import benchmark_utils
import aws_credentials
from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
from boto.s3.key import Key

BUCKET_NAME = "cs739_dynamofs"

# Get a feel for server-side latencies.
sampler = benchmark_utils.BenchmarkTimer()

# Backend driven by S3.
##############################################################
# NOTE. We currently do not do reference-counting for S3. That is,
# nuke() is the only way that key-value pairs can be removed from the
# S3Backend. Reference-counting for S3 would impose lots
# of extra overhead, since SimpleDB does not support atomic increments.
##############################################################
class S3Backend:
    
    def __init__(self):
        self.connection = S3Connection(aws_credentials.accessKey,
                                       aws_credentials.secretKey)
        self.bucket = self.connection.get_bucket(BUCKET_NAME)      


    def put(self, key, value):
        sampler.begin()
        try:
            s3Key = Key(self.bucket)
            s3Key.key = key
            s3Key.set_contents_from_string(value)
        finally:
            sampler.end()


    def get(self, key):
        sampler.begin()
        try:
            s3Key = Key(self.bucket)
            s3Key.key = key
            return s3Key.get_contents_as_string()
        except Exception as e:
            if e.error_message == 'The specified key does not exist.':
                # Translate this to a KeyError.
                raise KeyError
            else:
                raise e
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
        for key in self.bucket.list():
            self.bucket.delete_key(key)
                    
    def flush(self):
        pass # No-op.
