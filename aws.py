import os
import boto3
from errors import error_handler

region_default = "us-east-2"

class AWS:
    def __init__(self, region=region_default):
        # region, session, path
        self.region = region
        self.CreateSession(self.region) # declare self.session

    @error_handler()
    def CreateSession(self, region=None):
        if region is None:
            region=self.region
        self.session = boto3.Session(region_name=region)
        self.region = self.session.region_name

    @error_handler()
    def CreateResource(self, resource):
        self.resource = self.session.resource(resource)

    @error_handler()
    def CreateClient(self, client):
        self.client = self.session.client(client)

if __name__ == "__main__":
    pass