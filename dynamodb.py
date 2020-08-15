import os
import boto3
from botocore.exceptions import ClientError, ParamValidationError
import json

from aws import AWS, error_handler

class DynamoDB(AWS):
    def __init__(self, region=None):
        super().__init__(region=region)
        super().CreateResource("dynamodb")
        self.table = None
        self.tableAttributes = None

    @error_handler()
    def ValidateSchema(self, schema):
        return all(key in item.keys() for key in ("Name", "KeyType", "Type") for item in schema)

    @error_handler(ParamValidationError)
    def CreateTable(self, table_name, schema=[]):
        if schema == []:
            attribute_definitions = [{
                "AttributeName": "ID",
                "AttributeType": "N"
            }]
            key_schema = [{
                "AttributeName": "ID",
                "KeyType": "HASH"
            }]
        elif self.ValidateSchema(schema):
            attribute_definitions = [{
                "AttributeName": item["Name"],
                "AttributeType": item["Type"]
            } for item in schema]
            key_schema = [{
                "AttributeName": item["Name"],
                "KeyType": item["KeyType"]
            } for item in schema]
        else:
            raise Exception()
        self.resource.create_table(
            TableName=table_name, 
            AttributeDefinitions=attribute_definitions, 
            KeySchema=key_schema,
            ProvisionedThroughput={
                "ReadCapacityUnits":1,
                "WriteCapacityUnits":1
            }
        )
        self.table = self.resource.Table(table_name)
        self.tableAttributes = [x["AttributeName"] for x in self.table.attribute_definitions]

    @error_handler()
    def ListTables(self):
        print("Existing tables:")
        for table in list(self.resource.tables.all()):
            print(f"    {table.name}")

    @error_handler()
    def GetTable(self, table_name):
        self.table = self.resource.Table(table_name)
        self.tableAttributes = [x["AttributeName"] for x in self.table.attribute_definitions]
        return self.table

    @error_handler(ClientError)
    def UploadData(self, table_name, input=[]):
        if type(input)==str and input[-5:]==".json" and os.path.exists(input):
            with open(input, 'r') as fdata:
                items = json.load(fdata)
        else:
            items = json.load(input)
        entrybatchlist = []
        for item in items:
            entrybatchlist.append({'PutRequest': {'Item':item}})
        entrybatch = {table_name: entrybatchlist}
        response = self.resource.batch_write_item(RequestItems=entrybatch, ReturnItemCollectionMetrics='SIZE')
        print(response)
    
    @error_handler(ClientError)
    def DeleteData(self, table_name, input=[]):
        if type(input)==str and input[-5:]==".json" and os.path.exists(input):
            with open(input, 'r') as fdata:
                items = json.load(fdata)
        else:
            items = json.load(input)
        entrybatchlist = []
        for item in items:
            entrybatchlist.append({'DeleteRequest': {'Key':item}})
        entrybatch = {table_name: entrybatchlist}
        response = self.resource.batch_write_item(RequestItems=entrybatch, ReturnItemCollectionMetrics='SIZE')
        print(response)

    @error_handler(ClientError)
    def DeleteTable(self, table_name):
        response = self.GetTable(table_name).delete()
        print(response)

if __name__ == "__main__":
    pass