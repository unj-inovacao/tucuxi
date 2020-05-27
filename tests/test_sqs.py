import itertools
import logging

from tucuxi import Sqs

logger = logging.getLogger(__name__)


def test_send_message(sqs_url, objs):
    sqs = Sqs(sqs_url, region="us-east-1")
    logger.info(sqs.send_message(objs, delay=0))


def test_send_message_batch(sqs_url, objs):
    sqs = Sqs(sqs_url, region="us-east-1")
    bunch_objs = [*objs * 10]
    logger.info(sqs.send_message_batch(bunch_objs, raise_on_error=True))


def test_listen_to_message(sqs_url, objs):
    sqs = Sqs(sqs_url, region="us-east-1")
    assert objs == next(sqs.listen())[1]


def test_delete_message(sqs_url, objs):
    sqs = Sqs(sqs_url, region="us-east-1")
    receipt, _ = next(sqs.listen(auto_delete=False))
    logger.info(sqs.delete_message(receipt))


def test_delete_message_batch(sqs_url, objs):
    sqs = Sqs(sqs_url, region="us-east-1")
    res = itertools.islice(sqs.listen(max_number_of_messages=10, auto_delete=False), 15)
    receipts = list(map(lambda x: x[0], res))
    logger.info(sqs.delete_message_batch(receipts, raise_on_error=True))
