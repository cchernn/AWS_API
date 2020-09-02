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
        self.fields = []

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
        response = self.resource.create_table(
            TableName=table_name, 
            AttributeDefinitions=attribute_definitions, 
            KeySchema=key_schema,
            ProvisionedThroughput={
                "ReadCapacityUnits":1,
                "WriteCapacityUnits":1
            }
        )
        response.wait_until_exists()
        self.table = self.resource.Table(table_name)
        self.fields = [x["AttributeName"] for x in self.table.attribute_definitions]
        return {
            "Table": self.table.name,
            "Fields": self.fields
        }

    @error_handler(ClientError)
    def GetTables(self):
        return list(self.resource.tables.all())
        # return {
        #     "Tables": list(self.resource.tables.all())
        # }

    @error_handler(ClientError)
    def GetTableAllItems(self, table_name=None):
        if table_name is None:
            result = self.table.scan()['Items']
        else:
            result = self.resource.Table(table_name).scan()['Items']
        return result
        # return {
        #     "Items": result
        # }

    @error_handler(ClientError)
    def GetActiveTable(self, table_name):
        self.table = self.resource.Table(table_name)
        self.fields = [x["AttributeName"] for x in self.table.attribute_definitions]
        return self.table
        # return {
        #     "Table": self.table.name,
        #     "Fields": self.fields
        # }

    @error_handler(ClientError)
    def GetQueryItems(self, query, table_name=None):
        if table_name is None:
            workingTable = self.table
        else:
            workingTable = self.resource.Table(table_name)
        result = workingTable.get_item(Key = query)
        return result
        # return {
        #     "Items": result
        # }

    @error_handler(ClientError)
    def PutItems(self, input, table_name=None):
        if table_name is None:
            table_name = self.table.name
        if type(input)==str and input[-5:]==".json" and os.path.exists(input):
            with open(input, 'r') as fdata:
                items = json.load(fdata)
        else:
            items = json.loads(json.dumps(input))
        entrybatchlist = []
        for item in items:
            entrybatchlist.append({'PutRequest': {'Item':item}})
        max_item_count = len(entrybatchlist)
        item_count = 0
        max_items = 25
        unprocessedItems = []
        while item_count < max_item_count:
            if item_count+max_items >= max_item_count:
                tempbatchlist = entrybatchlist[item_count:item_count+max_item_count]
            else:
                tempbatchlist = entrybatchlist[item_count:item_count+max_items]
            entrybatch = {table_name: tempbatchlist}
            response = self.resource.batch_write_item(RequestItems=entrybatch, ReturnItemCollectionMetrics='SIZE')
            if len(response["UnprocessedItems"]) > 0:
                unprocessedItems.append(response["UnprocessedItems"]) 
            item_count += max_items
        return {
            "Table": table_name,
            "Items_Count": max_item_count,
            "Unprocessed_Items": unprocessedItems
        }
    
    @error_handler(ClientError)
    def DeleteItems(self, input, table_name=None):
        if table_name is None:
            table_name = self.table.name
        if type(input)==str and input[-5:]==".json" and os.path.exists(input):
            with open(input, 'r') as fdata:
                items = json.load(fdata)
        else:
            items = json.load(input)
        entrybatchlist = []
        for item in items:
            entrybatchlist.append({'DeleteRequest': {'Key':item}})
        max_item_count = len(entrybatchlist)
        item_count = 0
        max_items = 25
        unprocessedItems = []
        while item_count < max_item_count:
            if item_count+max_items >= max_item_count:
                tempbatchlist = entrybatchlist[item_count:item_count+max_item_count]
            else:
                tempbatchlist = entrybatchlist[item_count:item_count+max_items]
            entrybatch = {table_name: tempbatchlist}
            response = self.resource.batch_write_item(RequestItems=entrybatch, ReturnItemCollectionMetrics='SIZE')
            if len(response["UnprocessedItems"]) > 0:
                unprocessedItems.append(response["UnprocessedItems"]) 
            item_count += max_items
        return {
            "Table": table_name,
            "Items_Count": max_item_count,
            "Unprocessed_Items": unprocessedItems
        }

    @error_handler(ClientError)
    def DeleteTable(self, table_name):
        response = self.resource.Table(table_name).delete()
        return response

if __name__ == "__main__":
    pass