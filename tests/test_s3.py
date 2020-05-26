from pycloudutils.aws.s3 import S3_Client
import boto3
import logging

logger = logging.getLogger(__name__)



def test_set_get_object(s3_name, objs):
    s3 = S3_Client(s3_name)
    logger.info("Testing set_object and get_object")
    s3.set_object(objs[0]["key"], objs[0]["data"])
    assert objs[0]["data"] == s3.get_object(objs[0]["key"]).decode("utf8")


def test_get_all_objects(s3_name,objs):
    s3 = S3_Client(s3_name)
    logger.info("Testing get_all_s3_objects")
    s3.set_object(objs[1]["key"], objs[1]["data"])
    result = list(s3.get_all_s3_objects("T","."))
    assert result == [objs[0]["key"][:-4], objs[1]["key"][:-4]]

def test_by_prefix(s3_name,objs):
    s3 = S3_Client(s3_name)
    logger.info("Testing get_by prefix")
    assert objs[0]["key"] == next(s3.get_by_prefix(objs[0]["key"][:-5]))

def test_get_size(s3_name,objs):
    s3 = S3_Client(s3_name)
    logger.info("Testing get_size")
    assert 9 == s3.get_size(objs[0]["key"])

def test_view_tree(s3_name,objs):
    s3 = S3_Client(s3_name)
    logger.info("Testing view_tree")
    s3.view_tree()



    