import sys

# Initialize the classpath and import AWS Java classes.
sys.path.append("aws-java-sdk-1.3.7/lib/aws-java-sdk-1.3.7.jar")
from java.io import *
from java.util import *
from com.amazonaws import *
from com.amazonaws.auth import *
from com.amazonaws.services.dynamodb import *
from com.amazonaws.services.dynamodb.model import *

credentialsFile = "AwsCredentials.properties"
tableName = "data"


# Backend driven by DynamoDB. We use one table called "data", which has the
# following columns:
#    key (string)
#    refCount (number)
#    value (string)
class DynamoDBBackend:
    def __init__(self):
        # Initialize the client library, using the credentials.
        credentials = PropertiesCredentials(FileInputStream(credentialsFile))
        self.client = AmazonDynamoDBClient(credentials)
        
    # Makes an API Key object from a key string.
    def apiKey(self, key):
        return Key().withHashKeyElement(AttributeValue().withS(key))

    def put(self, key, value):
        key = self.apiKey(key)
        
        # Issue an UpdateItem request that atomically increments the refCount
        # and puts the value. This will transparently create the item if it
        # isn't there already.
        updates = HashMap()
        
        # Increment the refCount if it exists, or set it to 1 if it doesn't exist.
        updates.put("refCount", AttributeValueUpdate()
                    .withAction(AttributeAction.ADD)
                    .withValue(AttributeValue().withN("1")))
        
        # Simply put the value.
        updates.put("value", AttributeValueUpdate()
                    .withAction(AttributeAction.PUT)
                    .withValue(AttributeValue().withS(value)))
        
        request = (UpdateItemRequest()
                   .withTableName(tableName)
                   .withKey(key)
                   .withAttributeUpdates(updates))
        result = self.client.updateItem(request) # TODO handle errors
        # TODO collect capacity units consumed

    def get(self, key):
        key = self.apiKey(key)
        
        # Issue an eventually-consistent GetItem request.
        request = (GetItemRequest()
                   .withConsistentRead(False) # No strong consistency.
                   .withKey(key)
                   .withAttributesToGet("value"))
        result = self.client.getItem(request) # TODO handle errors
        # TODO collect capacity units consumed
        
        item = result.getItem()
        if not(item is None):
            return item.get("value").getS()
        
        else:
            # If the eventually consistent request failed, try a consistent GetItem request.
            request = request.withConsistendRead(True)
            result = self.client.getItem(request)
            # TODO collect capacity units consumed.
            
            if not(item is None):
                return item.get("value").getS()
            
            else: # The key must not exist in the database.
                raise KeyError

    def incRefCount(self, key):
        # TODO
        pass
        
    def decRefCount(self, key):
        # TODO
        pass
        