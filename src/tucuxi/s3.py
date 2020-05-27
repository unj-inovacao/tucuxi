import logging
from typing import Optional

import botocore

from .session import Session

logger = logging.getLogger(__name__)


class S3:
    def __init__(self, bucket_name: str, session: Optional[Session] = None):
        if not session:
            session = Session()
        sess = session.get_session()

        resource = sess.resource("s3")

        self.client = sess.client(
            "s3", config=botocore.client.Config(max_pool_connections=1000)
        )

        self.bucket_name = bucket_name
        self.client.head_bucket(Bucket=self.bucket_name)
        self.bucket = resource.Bucket(self.bucket_name)
        logger.info(f"{self.bucket} connected")

    def view_tree(self, start="", ident=4, _level=0):
        line = "│" + ident * " "
        item = "├" + (ident - 1) * "-"
        for folder in self.list_objects(start, "/", only_prefixes=True):
            print(line * _level + item + " " + folder[len(start) :])
            self.view_tree(ident, folder, _level + 1)
        print(f"\nBucket: {self.bucket_name}")

    def list_objects(self, prefix=None, delimiter=None, only_prefixes=False):
        list_kwargs = {
            "Bucket": self.bucket_name,
            "Prefix": prefix,
            "Delimiter": delimiter,
        }
        is_truncated = True
        while is_truncated:
            response = self.client.list_objects_v2(**list_kwargs)
            if only_prefixes:
                for content in response.get("CommonPrefixes", []):
                    yield content.get("Prefix")
            else:
                for content in response.get("Contents", []):
                    yield content.get("Key")
            is_truncated = response.get("IsTruncated")
            list_kwargs["ContinuationToken"] = response.get("NextContinuationToken")

    def get_object(
        self, obj_key, retries=0
    ):  # TODO Better exception handling and maybe async download
        try:
            file = self.client.get_object(Bucket=self.bucket_name, Key=obj_key)
            return file["Body"].read()
        except BaseException as e:
            logger.error(f"Error: {e}. Retrying...")
            if retries:
                return self.get_object(obj_key, retries=retries - 1)
            raise (e)

    def set_object(
        self, obj_key, obj_data
    ):  # TODO  More params, better exception handling and maybe async upload
        return self.client.put_object(
            Bucket=self.bucket_name, Key=obj_key, Body=obj_data
        )

    def get_size(self, obj_key):
        return self.bucket.Object(obj_key).content_length

    # TODO Get size with prefix and delimiter, like list

    def move_obj(self):  # TODO method to send a object to another bucket or path
        pass
