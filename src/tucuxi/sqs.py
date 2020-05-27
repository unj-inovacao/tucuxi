import json
import logging
import re
import time
from functools import reduce
from typing import Optional

from boltons.iterutils import chunked_iter

from .session import Session


logger = logging.getLogger(__name__)


class Sqs:
    def __init__(
        self, queue_url, region="us-east-1", session: Optional[Session] = None
    ):
        if not region:
            region = re.search(r"https://sqs\.(.*)\.a", queue_url).group(
                1
            )  # TODO Improve this
        if not session:
            session = Session(region_name=region)
        sess = session.get_session()
        self.client = sess.client("sqs")
        self.queue_url = queue_url

    def _batch(self, entries, key, operation, raise_on_error=False, apply=lambda x: x):
        res_list = []
        for i_chunk, chunk in enumerate(chunked_iter(entries, 10)):
            payload = [
                {"Id": str(i_chunk * 10 + i), key: apply(m)}
                for i, m in enumerate(chunk)
            ]
            res = operation(QueueUrl=self.queue_url, Entries=payload)
            print(res)
            if raise_on_error and res.get("Failed"):
                raise (Exception)
            res_list.append(res)
        return reduce(
            lambda c, r: {
                key: c.get(key, []) + r.get(key, []) for key in ["Successful", "Failed"]
            },
            res_list,
        )

    def send_message(self, message=None, delay=10):
        logger.debug(f"Sending message to {self.queue_url}")

        return self.client.send_message(
            QueueUrl=self.queue_url,
            DelaySeconds=delay,
            MessageBody=json.dumps(message),
        )

    def send_message_batch(self, messages, raise_on_error=False):
        return self._batch(
            messages,
            "MessageBody",
            self.client.send_message_batch,
            raise_on_error,
            json.dumps,
        )

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

    # TODO Implement decorator for listen

    def delete_message(self, receipt_handle):
        logger.debug(f"Deleting message {receipt_handle} from {self.queue_url}")
        return self.client.delete_message(
            QueueUrl=self.queue_url, ReceiptHandle=receipt_handle
        )

    def delete_message_batch(self, receipts, raise_on_error=False):
        return self._batch(
            receipts, "ReceiptHandle", self.client.delete_message_batch, raise_on_error
        )
