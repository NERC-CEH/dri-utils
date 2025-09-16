import logging
import urllib.parse

from botocore.client import BaseClient
from botocore.exceptions import ClientError
from mypy_boto3_s3.client import S3Client

from driutils.io.interfaces import ReaderInterface, WriterInterface

logger = logging.getLogger(__name__)


class S3Base:
    """Base class to reuse initializer"""

    _connection: S3Client
    """The S3 client used to perform the work"""

    def __init__(self, s3_client: S3Client) -> None:
        """Initializes

        Args:
            s3_client: The S3 client used to do work
        """

        if not isinstance(s3_client, BaseClient):
            raise TypeError(f"'s3_client must be a BaseClient, not '{type(s3_client)}'")

        self._connection = s3_client


class S3Reader(S3Base, ReaderInterface):
    """Class for handling file reads using the AWS S3 client"""

    def read(self, bucket_name: str, key: str) -> bytes:
        """
        Retrieves an object from an S3 bucket.

        If any step fails, it logs an error and re-raises the exception.

        Args:
            bucket_name: The name of the S3 bucket.
            key: The key (path) of the object within the bucket.

        Returns:
            bytes: raw bytes of the S3 object

        Raises:
            Exception: If there's any error in retrieving or parsing the object.
        """
        try:
            data = self._connection.get_object(Bucket=bucket_name, Key=key)
            return data["Body"].read()
        except (RuntimeError, ClientError) as e:
            logger.error(f"Failed to get {key} from {bucket_name}")
            logger.exception(e)
            raise e


class S3Writer(S3Base, WriterInterface):
    """Writes to an S3 bucket"""

    def write(self, bucket_name: str, key: str, body: bytes, tags: dict = None) -> None:
        """Uploads an object to an S3 bucket.

        This function attempts to upload a byte object to a specified S3 bucket
        using the provided S3 client. If the upload fails, it logs an error
        message and re-raises the exception.

        Args:
            bucket_name: The name of the S3 bucket.
            key: The key (path) of the object within the bucket.
            body: data to write to s3 object
            tags: Any tags to add to the object, Defaults to None.

        Raises:
            TypeError: If body is not bytes
            TypeError: If tags is not a dict
        """
        if not isinstance(body, bytes):
            raise TypeError(f"'body' must be 'bytes', not '{type(body)}'")

        # The tag-set must be encoded as URL Query parameters key1=value1&key2=value2
        if tags:
            if not isinstance(tags, dict):
                raise TypeError(f"'tags' must be 'dict' not '{type(tags)}'")

            tag_set = urllib.parse.urlencode(tags)

            self._connection.put_object(Bucket=bucket_name, Key=key, Body=body, Tagging=tag_set)
        else:
            self._connection.put_object(Bucket=bucket_name, Key=key, Body=body)


class S3ReaderWriter(S3Reader, S3Writer):
    """Class to handle reading and writing in S3"""
