from typing import Optional

import boto3


class Session:
    def __init__(
        self,
        access_key: Optional[str] = None,
        secret_access: Optional[str] = None,
        region_name: Optional[str] = None,
        profile_name: Optional[str] = None,
    ):

        self.sess = boto3.Session(
            access_key,
            secret_access,
            region_name=region_name,
            profile_name=profile_name,
        )

    def get_session(self):
        return self.sess

    def s3(self, bucket_name: str):
        from tucuxi import S3  # Sorry!

        return S3(bucket_name, self.sess)

    def sqs(self, queue_url, region=None):
        from tucuxi import Sqs  # Sorry!

        return Sqs(queue_url, region)
