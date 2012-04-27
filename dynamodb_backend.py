import sys
import os
import benchmark_utils
from boto.dynamodb.layer1 import Layer1
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
        
        finally:
            sampler.end()
        
        
    def decRefCount(self, key):
        sampler.begin()
        
        try:
            key = self._apiKey(key)
            
            # Issue an UpdateItem request to decrement the refCount.
            self._addToRefCount(key, -1)
            
            
            
            
            # TODO left off boto conversion here.
            
            
            
            
            
            # Atomically delete the item, if its reference count is zero.
            expectation = HashMap()
            
            # Check refCount.
            expectation.put(REF_COUNT, ExpectedAttributeValue()
                            .withValue(AttributeValue().withN("0")))
            
            request = (DeleteItemRequest()
                       .withTableName(TABLE_NAME)
                       .withKey(key)
                       .withExpected(expectation))
            
            try:
                result = self.client.deleteItem(request)
                self.useCapacity(result)
            except ConditionalCheckFailedException:
                pass # Do nothing. This just means the refCount was positive.
            
        finally:    
            sampler.end()
    
    
    def nuke(self):
        while True:
            # Issue a Scan request to learn the contents of the table.
            request = (ScanRequest()
                       .withTableName(TABLE_NAME)
                       .withAttributesToGet(Collections.singleton(KEY)))
            result = self.client.scan(request)
            self.useCapacity(result)
            
            # If there are no items in the table, return.
            if result.getCount() == 0:
                return # We have emptied the table.
            else:
                itemsToDelete = result.getItems()
                
                # Delete the items, sending multiple BatchWriteRequests if the number
                # of items exceeds the ceiling.
                total = itemsToDelete.size()
                index = 0
                while index < total:
                    # Build a list of delete requests for the records returned by the scan.
                    writeRequests = ArrayList()
                    batchSize = 0
                    while (index < total) and (batchSize < MAX_BATCH_SIZE):
                        key = self._apiKey(itemsToDelete.get(index).get(KEY).getS())
                        writeRequests.add(WriteRequest()
                                          .withDeleteRequest(DeleteRequest()
                                                             .withKey(key)))
                        index += 1
                        batchSize += 1
                    
                    # Issue a BatchWriteItems request to delete the scanned items.
                    request = (BatchWriteItemRequest()
                               .withRequestItems(Collections.singletonMap(TABLE_NAME, writeRequests)))
                    result = self.client.batchWriteItem(request).getResponses().get(TABLE_NAME)
                    self.useCapacity(result)
                    
    def flush(self):
        pass # No-op.
