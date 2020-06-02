"""Module related with AWS Access keys and configurations, you should use this mostly if you are not using your enviroment keys"""
from typing import Any
from typing import Optional

import boto3


class Session:
    """AWS Session Client."""

    def __init__(
        self,
        access_key: Optional[str] = None,
        secret_access: Optional[str] = None,
        region_name: Optional[str] = None,
        profile_name: Optional[str] = None,
    ):
        """[summary]

        Args:
            access_key (Optional[str], optional): [description]. Defaults to None.
            secret_access (Optional[str], optional): [description]. Defaults to None.
            region_name (Optional[str], optional): [description]. Defaults to None.
            profile_name (Optional[str], optional): [description]. Defaults to None.
        """
        self.sess = boto3.Session(
            access_key,
            secret_access,
            region_name=region_name,
            profile_name=profile_name,
        )

    def get_session(self) -> Any:
        """[summary]

        Returns:
            Any: [description]
        """
        return self.sess

    def s3(self, bucket_name: str) -> Any:
        """[summary]

        Args:
            bucket_name (str): [description]

        Returns:
            Any: [description]
        """
        from tucuxi import S3  # Sorry!

        return S3(bucket_name, self)

    def sqs(self, queue_url: str, region: str = "us-east-1") -> Any:
        """[summary]

        Args:
            queue_url (str): [description]
            region (str): [description]. Defaults to "us-east-1".

        Returns:
            Any: [description]
        """
        from tucuxi import Sqs  # Sorry!

        return Sqs(queue_url, region, self)
