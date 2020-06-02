"""Tests for the tucuxi.session Module"""
from tucuxi import S3
from tucuxi import Session
from tucuxi import Sqs


def test_get_session() -> None:
    """[summary]"""
    sess = Session()
    gsess = sess.get_session()
    assert gsess is sess.sess


def test_s3(s3_name: str) -> None:
    """[summary]

    Args:
        s3_name (str): [description]
    """
    sess = Session()
    s3 = sess.s3(s3_name)
    assert isinstance(s3, S3)


def test_sqs(sqs_url: str) -> None:
    """[summary]

    Args:
        sqs_url (str): [description]
    """
    sess = Session()
    sqs = sess.sqs(sqs_url)
    assert isinstance(sqs, Sqs)
