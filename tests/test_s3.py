"""Tests for the tucuxi.s3 Module using some fixtures from conftest.py"""
import logging
from typing import Dict
from typing import List

from tucuxi import S3

logger = logging.getLogger(__name__)


def test_set_get_object(s3_name: str, objs: List[Dict[str, str]]) -> None:
    """[summary]

    Args:
        s3_name (str): [description]
        objs (List[Dict[str, str]]): [description]
    """
    s3 = S3(s3_name)
    logger.info("Testing set_object and get_object")
    s3.set_object(objs[0]["key"], objs[0]["data"])
    assert objs[0]["data"] == s3.get_object(objs[0]["key"]).decode("utf8")


def test_list_objects(s3_name: str, objs: List[Dict[str, str]]) -> None:
    """[summary]

    Args:
        s3_name (str): [description]
        objs (List[Dict[str, str]]): [description]
    """
    s3 = S3(s3_name)
    logger.info("Testing get_all_s3_objects")
    s3.set_object(objs[1]["key"], objs[1]["data"])
    result = list(s3.list_objects("T"))
    assert result == [objs[0]["key"], objs[1]["key"]]


def test_list_objects_prefix(s3_name: str, objs: List[Dict[str, str]]) -> None:
    """[summary]

    Args:
        s3_name (str): [description]
        objs (List[Dict[str, str]]): [description]
    """
    s3 = S3(s3_name)
    logger.info("Testing get_by prefix")
    assert objs[0]["key"][:-4] == next(s3.list_objects("T", "."))


def test_get_size(s3_name: str, objs: List[Dict[str, str]]) -> None:
    """[summary]

    Args:
        s3_name (str): [description]
        objs (List[Dict[str, str]]): [description]
    """
    s3 = S3(s3_name)
    logger.info("Testing get_size")
    assert 9 == s3.get_size(objs[0]["key"])


def test_view_tree(s3_name: str, objs: List[Dict[str, str]]) -> None:
    """[summary]

    Args:
        s3_name (str): [description]
        objs (List[Dict[str, str]]): [description]
    """
    s3 = S3(s3_name)
    logger.info("Testing view_tree")
    s3.view_tree()
