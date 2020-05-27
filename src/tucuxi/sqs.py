import json
import logging
import re
import time
from typing import Optional

from botocore.exceptions import ClientError

from .session import Session


logger = logging.getLogger(__name__)


class Sqs:
    def __init__(self, queue_url, region=None, session: Optional[Session] = None):
        if not session:
            session = Session()
        sess = session.get_session()
        if not region:
            region = re.search(r"https://sqs\.(.*)\.a", queue_url).group(
                1
            )  # TODO Improve this

        self.client = sess.client("sqs")
        self.queue_url = queue_url

    def delete_message(self, receipt_handle):
        try:
            return self.client.delete_message(
                QueueUrl=self.queue_url, ReceiptHandle=receipt_handle
            )
        except ClientError as e:
            logging.error(e)
            return None

    # TODO Implement a batch message sender
    def send_message(self, message=None, delay=10):
        logger.debug(f"Sending message to {self.queue_url}")
        try:
            return self.client.send_message(
                QueueUrl=self.queue_url,
                DelaySeconds=delay,
                MessageBody=json.dumps(message),
            )
        except ClientError as e:
            logging.error(e)
            return None

    def listen(
        self, wait_time=0, max_number_of_messages=1, poll_interval=30, auto_delete=True
    ):
        # TODO Look for other packages to have ideas. Example, auto sending to error queue.
        logger.info(f"Starting to listen to {self.queue_url}")
        while True:
            # calling with WaitTimeSecconds of zero show the same behavior as
            # not specifiying a wait time, ie: short polling
            messages = self.client.receive_message(
                QueueUrl=self.queue_url,
                WaitTimeSeconds=wait_time,
                MaxNumberOfMessages=max_number_of_messages,
            )
            if "Messages" in messages:
                logger.info("{} messages received".format(len(messages["Messages"])))
                for m in messages["Messages"]:
                    receipt_handle = m["ReceiptHandle"]
                    m_body = m["Body"]
                    # TODO Better exception handling
                    try:
                        params_dict = json.loads(m_body)
                    except BaseException:
                        logger.warning(
                            "Unable to parse message - JSON is not formatted properly"
                        )
                        continue
                    logger.debug(f"Yielding message {receipt_handle}")
                    if auto_delete:
                        self.delete_message(receipt_handle)
                    yield receipt_handle, params_dict

            else:
                if poll_interval:
                    time.sleep(poll_interval)
                else:
                    break
