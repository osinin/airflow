import boto3
import os


class ClientS3:

    def __init__(self, url, access_key, secret_key, bucket, file_extension):
        self.url = url
        self.s3_access_key = access_key
        self.s3_secret_key = secret_key
        self.bucket = bucket
        self.file_extension = file_extension

    def __get_boto_client__(self):
        return boto3.client('s3', aws_access_key_id=self.s3_access_key,
                          aws_secret_access_key=self.s3_secret_key,
                          endpoint_url=self.url)

    def download_file(self, file_name, local_dir):
        s3_client = self.__get_boto_client__()
        s3_client.download_file(self.bucket, file_name, os.path.join(local_dir, file_name.split("/")[-1]))

    def list_dir(self, prefix):
        s3_client = self.__get_boto_client__()
        return s3_client.list_objects(Bucket=self.bucket, Prefix=prefix)

    def list_dir_with_paginator(self, prefix):
        """
        Listing S3 dir without limitations.
        :param prefix
        :return: array of files from S3
        """
        s3_client = self.__get_boto_client__()
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)
        return [obj for page in pages for obj in page['Contents']]

    def upload_file(self, file_name, bucket, path):
        s3_client = self.__get_boto_client__()
        return s3_client.upload_file(file_name, bucket, path)

    def get_object(self, file_name):
        s3_client = self.__get_boto_client__()
        return s3_client.get_object(Bucket=self.bucket, Key=file_name)
