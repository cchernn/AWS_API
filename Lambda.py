import os
import boto3
import zipfile
from io import BytesIO
from botocore.exceptions import ClientError, ParamValidationError

from aws import AWS, error_handler

class Lambda(AWS):
    def __init__(self, region=None):
        super().__init__(region=region)
        super().CreateClient("lambda")
        self.function = None,
        self.runtime = None,
        self.role = None,
        self.handler = None

    @error_handler()
    def zipFiles(self, files):
        buf = BytesIO()
        if type(files) == list:
            for file in files:
                with zipfile.ZipFile(buf, 'a') as z: 
                    z.write(file)
        buf.seek(0)
        return buf.read()

    @error_handler(ClientError)
    def CreateFunction(self, function_name, description, code):
        response = self.client.create_function(
            FunctionName = function_name,
            Runtime = self.runtime,
            Role = self.role,
            Handler = f"{self.handler}.lambda_handler",
            Description = description,
            Code = code
        )
        self.function = function_name
        return response

    @error_handler(ClientError)
    def GetFunctions(self):
        return self.client.list_functions()

    @error_handler(ClientError)
    def UpdateFunction(self, code, function_name=None):
        if function_name is None:
            function_name = self.function
        response = self.client.update_function_code(
            FunctionName = function_name,
            ZipFile = code
        )
        return response

    @error_handler(ClientError)
    def RunFunction(self, payload=None,function_name=None):
        if function_name is None:
            function_name = self.function
        response = self.client.invoke(
            FunctionName = function_name,
            Payload = payload
        )
        return response

    @error_handler(ClientError)
    def DeleteFunction(self, function_name=None):
        if function_name is None:
            function_name = self.function
        response = self.client.delete_function(FunctionName=function_name)
        return response

if __name__ == "__main__":
    pass
