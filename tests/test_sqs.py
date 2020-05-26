from pycloudutils.aws.sqs import Sqs_Client
import boto3
import logging

logger = logging.getLogger(__name__)

def test_send_message(sqs_url, objs):
    sqs = Sqs_Client(sqs_url, region="us-east-1")

    logger.info("Testing send message")
    logger.info(sqs.send_message(objs, delay=0))


def test_listen_to_message(sqs_url,objs):
    sqs = Sqs_Client(sqs_url, region="us-east-1")

    logger.info("Testing listen to message")
    sqs = Sqs_Client(sqs_url, region="us-east-1")
    assert objs == next(sqs.listen())