from time import sleep
import benchmark_utils
from boto.dynamodb.layer1 import Layer1
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
import aws_credentials

TABLE_NAME = "data"

# We use single-character attribute names to save space in requests.
KEY = "k"
VALUE = "v"
REF_COUNT = "r"

# Thresholds mandated by Amazon.
MAX_BATCH_SIZE = 25 # Maximum number of items in a BatchWriteItemsRequest.

# Get a feel for server-side latencies.
sampler = benchmark_utils.BenchmarkTimer()

# Backend driven by DynamoDB. We use one table called "data", which has the
# following columns:
#    key (string)
#    refCount (number)
#    value (string)
class DynamoDBBackend:
    
    def __init__(self):
        self.client = Layer1(aws_credentials.accessKey,
                             aws_credentials.secretKey)                                                  
        self.capacityUsed = 0.0    
    
        
    # Makes an API Key object from a key string.
    def _apiKey(self, key):
        if (key is None) or (len(key) == 0):
            # Empty keys are not permitted by Amazon.
            raise KeyError
        return {'HashKeyElement':{'S':key}}    
    
    
    # Increments the counter of capacity used.
    def useCapacity(self, result):
        self.capacityUsed += result['ConsumedCapacityUnits']


    def put(self, key, value):
        sampler.begin()
        
        try:
            # Issue an UpdateItem request to increment the refCount, putting the value
            # if necessary.
            key = self._apiKey(key)
            updates = {REF_COUNT:{'Value': {'N':'1'},
                                  'Action': 'ADD'},
                       VALUE:{'Value':{'S':value},
                              'Action': 'PUT'}
                       }
            result = self.client.update_item(TABLE_NAME, key, updates)
            self.useCapacity(result)
        
        finally:
            sampler.end()


    def get(self, key):
        sampler.begin()
        
        try:
            # Issue an eventually-consistent GetItem request.
            key = self._apiKey(key)
            result = self.client.get_item(TABLE_NAME, key, [VALUE], consistent_read=False)
            self.useCapacity(result)
            
            item = result['Item']
            if not(item is None):
                return item[VALUE]['S']
                    
            else:
                # If the eventually consistent request failed, try a consistent GetItem request.
                result = self.client.get_item(TABLE_NAME, key, [VALUE], consistent_read=True)
                self.useCapacity(result)
                
                item = result['Item']
                if not(item is None):
                    return item[VALUE]['S']
                
                else: # The key must not exist in the database.
                    raise KeyError
                
        except DynamoDBKeyNotFoundError:
            raise KeyError
        finally:
            sampler.end()


    # Issues a request to add a specific delta value (integer) to the refCount for a key.
    def _addToRefCount(self, key, delta):
        # Issue an UpdateItem request to increment the refCount.
        updates = {REF_COUNT:{'Value': {'N':str(delta)},
                              'Action': 'ADD'},
                   }
        result = self.client.update_item(TABLE_NAME, key, updates)
        self.useCapacity(result)
        

    def incRefCount(self, key):
        sampler.begin()
        
        try:
            key = self._apiKey(key)
                    
            # Issue an UpdateItem request to increment the refCount.
            self._addToRefCount(key, 1)
        
        except DynamoDBKeyNotFoundError:
            raise KeyError
        finally:
            sampler.end()
        
        
    def decRefCount(self, key):
        sampler.begin()
        
        try:
            key = self._apiKey(key)
            
            # Issue an UpdateItem request to decrement the refCount.
            self._addToRefCount(key, -1)
            
            # Atomically delete the item, if its reference count is zero.
            expectation = {REF_COUNT:{'Value':{'N':'0'}}}
        
            try:    
                result = self.client.delete_item(TABLE_NAME, key, expectation)
                self.useCapacity(result)
            except Exception as e:
                if e.error_message == 'The conditional request failed':
                    pass # The conditional check for a zero refCount must have failed; this is OK.
                else: # This is some other error, so propagate it.
                    raise e
        
        except DynamoDBKeyNotFoundError:
            raise KeyError
        finally:    
            sampler.end()
    
    
    def nuke(self):
        # Delete and re-create the table.
        try:
            self.client.delete_table(TABLE_NAME)
            
            # Wait for the table to be deleted.
            while True:
                sleep(0.01)
                try:
                    self.client.describe_table(TABLE_NAME)
                except:
                    break # The table must not exist anymore.
                
        except:
            pass # The table must have been deleted already.
        
        # Re-create the table.
        self.client.create_table(TABLE_NAME,
                                 {'HashKeyElement':{'AttributeName':KEY,
                                                    'AttributeType':'S'}},
                                 {'ReadCapacityUnits':10,
                                  'WriteCapacityUnits':5})
        
        # Wait for the table to be created.
        while True:
            sleep(0.01)
            try:
                result = self.client.describe_table(TABLE_NAME)
                if result['Table']['TableStatus'] == 'ACTIVE':
                    break
            except:
                pass # The table must not exist quite yet.
        
                    
    def flush(self):
        pass # No-op.
