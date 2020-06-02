"""Some useful high-level methods to interact with AWS S3."""
import json
import logging
import re
import time
from functools import reduce
from typing import Any
from typing import Callable
from typing import Dict
from typing import Generator
from typing import List
from typing import Optional
from typing import Tuple

from boltons.iterutils import chunked_iter

from .session import Session


logger = logging.getLogger(__name__)


class Sqs:
    """SQS Client."""

    def __init__(
        self,
        queue_url: str,
        region: str = "us-east-1",
        session: Optional[Session] = None,
    ) -> None:
        """[summary]

        Args:
            queue_url (str): [description]
            region (str): [description]. Defaults to "us-east-1".
            session (Optional[Session]): [description]. Defaults to None.
        """
        if not region:
            region = re.search(r"https://sqs\.(.*)\.a", queue_url).group(  # type: ignore
                1
            )
        if not session:
            session = Session()
        sess = session.get_session()
        self.client = sess.client("sqs", region_name=region)
        self.queue_url = queue_url

    def _batch(
        self,
        entries: Any,
        key: str,
        operation: Callable[..., Dict[str, str]],
        raise_on_error: bool = False,
        apply: Callable[..., Any] = lambda x: x,
    ) -> Dict[str, List[bool]]:
        """[summary]

        Args:
            entries (Any): [description]
            key (str): [description]
            operation (Callable[..., Dict[str, str]]): [description]
            raise_on_error (bool): [description]. Defaults to False.
            apply (Callable[..., Any]): [description]. Defaults to lambdax:x.

        Returns:
            Dict[str, List[bool]]: [description]

        Raises:
            Exception
        """
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
            res_list,  # type: ignore
        )

    def send_message(self, message: Any, delay: int = 10) -> Any:
        """[summary]

        Args:
            message (Any): [description]
            delay (int): [description]. Defaults to 10.

        Returns:
            Any: [description]
        """
        logger.debug(f"Sending message to {self.queue_url}")
        return self.client.send_message(
            QueueUrl=self.queue_url,
            DelaySeconds=delay,
            MessageBody=json.dumps(message),
        )

    def send_message_batch(
        self, messages: List[Any], raise_on_error: bool = False
    ) -> Dict[str, List[bool]]:
        """[summary]

        Args:
            messages (List[Any]): [description]
            raise_on_error (bool): [description]. Defaults to False.

        Returns:
            Dict[str, List[bool]]: [description]
        """
        return self._batch(
            messages,
            "MessageBody",
            self.client.send_message_batch,
            raise_on_error,
            json.dumps,
        )

    def listen(
        self,
        wait_time: int = 0,
        max_number_of_messages: int = 1,
        poll_interval: int = 30,
        auto_delete: bool = True,
    ) -> Generator[Tuple[str, Any], None, None]:
        """[summary]

        Args:
            wait_time (int): [description]. Defaults to 0.
            max_number_of_messages (int): [description]. Defaults to 1.
            poll_interval (int): [description]. Defaults to 30.
            auto_delete (bool): [description]. Defaults to True.

        Yields:
            Generator[tuple]: [description]
        """
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

    def delete_message(self, receipt_handle: str) -> Any:
        """[summary]

        Args:
            receipt_handle (str): [description]

        Returns:
            Any: [description]
        """
        logger.debug(f"Deleting message {receipt_handle} from {self.queue_url}")
        return self.client.delete_message(
            QueueUrl=self.queue_url, ReceiptHandle=receipt_handle
        )

    def delete_message_batch(
        self, receipts: List[str], raise_on_error: bool = False
    ) -> Dict[str, List[bool]]:
        """[summary]

        Args:
            receipts (List[str]): [description]
            raise_on_error (bool): [description]. Defaults to False.

        Returns:
            Dict[str, List[bool]]: [description]
        """
        return self._batch(
            receipts, "ReceiptHandle", self.client.delete_message_batch, raise_on_error
        )
