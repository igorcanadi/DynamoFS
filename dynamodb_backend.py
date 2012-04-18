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

    def put(self, key, value):
        # Build an UpdateItem request that atomically increments the refCount
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
                   .withKey(Key().withHashKeyElement(AttributeValue().withS(key)))
                   .withAttributeUpdates(updates))
        
        self.client.updateItem(request) # TODO handle errors

    def get(self, key):
        pass

    def incRefCount(self, key):
        pass
        
    def decRefCount(self, key):
        pass
        