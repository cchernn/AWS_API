import os
import sys
import uuid
import boto3
from botocore.exceptions import ClientError

from aws import AWS, error_handler

class S3(AWS):
    def __init__(self, region=None, path=None):
        super().__init__(region=region)
        super().CreateResource("s3") # declare self.resource
        self.bucket = None
        self.path = os.getenv("HOME") if path is None else path

    @error_handler()
    def CreateBucketName(self, bucket_prefix):
        return "".join([bucket_prefix, str(uuid.uuid4())])

    @error_handler(ClientError)
    def CreateBucket(self, bucket_name, region=None):
        if region is None:
            location = {"LocationConstraint": self.region}
        else:
            location = {"LocationConstraint": region}
        bucket_name = self.CreateBucketName(bucket_prefix=bucket_name)
        self.resource.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
        self.bucket = self.resource.Bucket(bucket_name)
        return {
            "Bucket": bucket_name
        }

    @error_handler(ClientError)
    def GetBuckets(self):
        return list(self.resource.buckets.all())
        # return {
        #     "Buckets": list(self.resource.buckets.all())
        # }

    @error_handler(ClientError)
    def GetObjects(self, bucket_name=None):
        if bucket is None:
            bucket = self.bucket
        else:
            bucket = self.resource.Bucket(bucket)
        objs = bucket.objects.all()
        return objs
        # return {
        #     "Objects": objs
        # }

    @error_handler(ClientError)
    def GetActiveBucket(self, bucket_name):
        response = self.resource.meta.client.head_bucket(Bucket=bucket_name)
        if response:
            self.bucket = self.resource.Bucket(bucket_name)
            return self.bucket
        else:
            return None

    @error_handler()
    def LocalFileExists(self, file_input):
        if not os.path.isabs(file_input): 
            file_input = os.path.join(self.path, file_input)
        if os.path.commonpath([file_input, self.path]) == self.path:
            file_key = os.path.relpath(file_input, self.path)
            if os.path.exists(file_input):
                return (file_input, file_key)
            else:
                return None
        else:
            return None

    @error_handler(ClientError)
    def UploadFile(self, bucket_name, file_input):
        if bucket_name is not None:
            bucket = self.resource.Bucket(bucket_name)
        else:
            bucket = self.bucket
        validFile = self.LocalFileExists(file_input)
        if validFile is not None:
            file_path, file_key = validFile
            bucket.upload_file(file_path, file_key)
            return {
                "Bucket": bucket.name,
                "File_Uploaded": file_key
            }
        else:
            return None

    @error_handler(ClientError)
    def UploadFiles(self, bucket_name, folder_path):
        folder_path = folder_path + "/" if folder_path[-1] != "/" else folder_path
        for r,_,f in os.walk(folder_path):
            for file in f:
                file_path = os.path.join(r, file)
                self.UploadFile(bucket_name, file_path)

    @error_handler(ClientError)
    def DeleteBucketContents(self, bucket_name):
        res = []
        bucket = self.resource.Bucket(bucket_name)
        for obj_version in bucket.object_versions.all():
            res.append({'Key': obj_version.key,
                        'VersionId': obj_version.id})
        response = bucket.delete_objects(Delete={'Objects': res})
        return response

    @error_handler(ClientError)
    def DeleteBucket(self, bucket_name):
        response = self.resource.Bucket(bucket_name).delete()
        return response

    @error_handler(ClientError)
    def DeleteFile(self, bucket_name, file_name):
        response = self.resource.Object(bucket_name, file_name).delete()
        return response

if __name__ == "__main__":
    pass
    



