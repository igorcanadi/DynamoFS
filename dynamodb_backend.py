import sys
import os
   
# Adds all the jar files in a directory to the path. Setting "recursive" to True
# will cause this method to descending into directories, searching for JARs.
def addJarsFrom(dirPath, recursive):
    jars = os.listdir(dirPath)
    for jar in jars:
        jarPath = dirPath + "/" + jar
        if os.path.isdir(jarPath):
            if recursive:
                addJarsFrom(jarPath, recursive)            
        elif jar.endswith(".jar"):
            sys.path.append(jarPath)

# Initialize the classpath and import AWS Java classes.
awsLib = "aws-java-sdk-1.3.8"
addJarsFrom(awsLib + "/lib", False)
addJarsFrom(awsLib + "/third-party", True)

from java.io import *
from java.util import *
from com.amazonaws import *
from com.amazonaws.auth import *
from com.amazonaws.services.dynamodb import *
from com.amazonaws.services.dynamodb.model import *

CREDENTIALS_FILE = "AwsCredentials.properties"
TABLE_NAME = "data"

# We use single-character attribute names to save space in requests.
KEY = "k"
VALUE = "v"
REF_COUNT = "r"


# Backend driven by DynamoDB. We use one table called "data", which has the
# following columns:
#    key (string)
#    refCount (number)
#    value (string)
class DynamoDBBackend:
    
    def __init__(self):
        # Initialize the client library, using the credentials.
        credentials = PropertiesCredentials(FileInputStream(CREDENTIALS_FILE))
        self.client = AmazonDynamoDBClient(credentials)
        self.capacityUsed = 0.0
    
        
    # Makes an API Key object from a key string.
    def apiKey(self, key):
        return Key().withHashKeyElement(AttributeValue().withS(key))
    
    
    # Increments the counter of capacity used.
    def useCapacity(self, result):
        self.capacityUsed += result.getConsumedCapacityUnits()


    def put(self, key, value):
        key = self.apiKey(key)
        
        # Issue an UpdateItem request that atomically increments the refCount
        # and puts the value. This will transparently create the item if it
        # isn't there already.
        updates = HashMap()
        
        # Increment the refCount if it exists, or set it to 1 if it doesn't exist.
        updates.put(REF_COUNT, AttributeValueUpdate()
                    .withAction(AttributeAction.ADD)
                    .withValue(AttributeValue().withN("1")))
        
        # Simply put the value.
        updates.put(VALUE, AttributeValueUpdate()
                    .withAction(AttributeAction.PUT)
                    .withValue(AttributeValue().withS(value)))
        
        request = (UpdateItemRequest()
                   .withTableName(TABLE_NAME)
                   .withKey(key)
                   .withAttributeUpdates(updates))
        result = self.client.updateItem(request)
        self.useCapacity(result)


    def get(self, key):
        key = self.apiKey(key)
        
        # Issue an eventually-consistent GetItem request.
        request = (GetItemRequest()
                   .withTableName(TABLE_NAME)
                   .withConsistentRead(False) # No strong consistency.
                   .withKey(key)
                   .withAttributesToGet(Collections.singleton(VALUE)))
        result = self.client.getItem(request)
        self.useCapacity(result)
        
        item = result.getItem()
        if not(item is None):
            return item.get(VALUE).getS()
        
        else:
            # If the eventually consistent request failed, try a consistent GetItem request.
            request = request.withConsistentRead(True)
            result = self.client.getItem(request)
            self.useCapacity(result)
            
            if not(item is None):
                return item.get(VALUE).getS()
            
            else: # The key must not exist in the database.
                raise KeyError


    # Issues a request to add a specific delta value (integer) to the refCount for a key.
    def addToRefCount(self, key, delta):
        # Issue an UpdateItem request to increment the refCount.
        updates = HashMap()
        
        # Increment the refCount.
        updates.put(REF_COUNT, AttributeValueUpdate()
                    .withAction(AttributeAction.ADD)
                    .withValue(AttributeValue().withN(str(delta))))
        
        request = (UpdateItemRequest()
                   .withTableName(TABLE_NAME)
                   .withKey(key)
                   .withAttributeUpdates(updates))
        result = self.client.updateItem(request)
        self.useCapacity(result)


    def incRefCount(self, key):
        key = self.apiKey(key)
        
        # Issue an UpdateItem request to increment the refCount.
        self.addToRefCount(key, 1)
        
        
    def decRefCount(self, key):
        key = self.apiKey(key)
        
        # Issue an UpdateItem request to decrement the refCount.
        self.addToRefCount(key, -1)
        
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
                # Build a list of delete requests, one for each record returned by
                # the scan.
                writeRequests = ArrayList()
                for item in result.getItems():
                    key = self.apiKey(item.get(KEY).getS())
                    writeRequests.add(WriteRequest()
                                      .withDeleteRequest(DeleteRequest()
                                                         .withKey(key)))
                
                # Issue a BatchWriteItems request to delete the scanned items.
                request = (BatchWriteItemRequest()
                           .withRequestItems(Collections.singletonMap(TABLE_NAME, writeRequests)))
                result = self.client.batchWriteItem(request).getResponses().get(TABLE_NAME)
                self.useCapacity(result)
                