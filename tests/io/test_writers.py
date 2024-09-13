import unittest
from unittest.mock import MagicMock
from driutils.io.write import S3Writer
import boto3
from botocore.client import BaseClient
from mypy_boto3_s3.client import S3Client
from parameterized import parameterized

class TestS3Writer(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:

        cls.s3_client: S3Client = boto3.client("s3", endpoint_url="http://localhost:4566") #type: ignore

    def test_s3_client_type(self):
        """Returns an object if s3_client is of type `boto3.client.s3`, otherwise
        raises an error"""

        # Happy path
        writer = S3Writer(self.s3_client)

        self.assertIsInstance(writer.s3_client, BaseClient)

        # Bad path
        
        with self.assertRaises(TypeError):
            S3Writer("not an s3 client") #type: ignore


    @parameterized.expand([1, "body", 1.123, {"key": b"bytes"}])
    def test_error_raises_if_write_without_bytes(self, body):
        """Tests that a type error is raised if the wrong type body used"""

        writer = S3Writer(self.s3_client)
        writer.s3_client = MagicMock()
        with self.assertRaises(TypeError):
            writer.write("bucket", "key", body)
        
        writer.s3_client.put_object.assert_not_called()

    def test_write_called(self):
        """Tests that the writer can be executed"""

        body = b"Test data"

        writer = S3Writer(self.s3_client)
        writer.s3_client = MagicMock()
        writer.write("bucket", "key", body)

        writer.s3_client.put_object.assert_called_once_with(Bucket="bucket", Key="key", Body=body)
        