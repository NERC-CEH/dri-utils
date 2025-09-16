import unittest
from typing import Any
from unittest.mock import MagicMock

import boto3
import moto
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from mypy_boto3_s3.client import S3Client
from parameterized import parameterized

from driutils.io.aws import S3Reader, S3Writer

BUCKET_NAME = "test_bucket"
TEST_KEY = "test_key"
TEST_OBJECT = b'{ "test": "object" }'
TEST_TAGS = {"tag1": "test1", "tag2": "test2"}


@moto.mock_aws
class TestS3Writer(unittest.TestCase):
    def setUp(self) -> None:
        self.s3_client: S3Client = boto3.client("s3")  # type: ignore

        self.s3_client.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

    def test_s3_client_type(self) -> None:
        """Returns an object if s3_client is of type `boto3.client.s3`, otherwise
        raises an error"""

        # Happy path
        writer = S3Writer(self.s3_client)

        self.assertIsInstance(writer._connection, BaseClient)

        # Bad path

        with self.assertRaises(TypeError):
            S3Writer("not an s3 client")  # type: ignore

    @parameterized.expand([1, "body", 1.123, {"key": b"bytes"}])
    def test_error_raises_if_write_without_bytes(self, body: Any) -> None:
        """Tests that a type error is raised if the wrong type body used"""

        writer = S3Writer(self.s3_client)
        with self.assertRaises(TypeError):
            writer.write(BUCKET_NAME, TEST_KEY, body)

        writer._connection.put_object.assert_not_called()

    def test_error_raised_if_tags_not_dict(self) -> None:
        """Test type error is raised if tags argument not a dict"""

        writer = S3Writer(self.s3_client)
        with self.assertRaises(TypeError):
            writer.write(BUCKET_NAME, TEST_KEY, TEST_OBJECT, "incorrect_tag_format")

        writer._connection.put_object.assert_not_called()

    def test_write_called(self) -> None:
        """Tests that the writer can be executed"""

        writer = S3Writer(self.s3_client)
        writer.write(BUCKET_NAME, TEST_KEY, TEST_OBJECT)

        writer._connection.put_object.assert_called_once_with(Bucket=BUCKET_NAME, Key=TEST_KEY, Body=TEST_OBJECT)

    def test_object_written(self) -> None:
        """Test the object is written correctly"""
        writer = S3Writer(self.s3_client)

        writer.write(BUCKET_NAME, TEST_KEY, TEST_OBJECT, TEST_TAGS)
        objectBody = self.s3_client.get_object(Bucket=BUCKET_NAME, Key=TEST_KEY)["Body"].read()
        object_tags = self.s3_client.get_object_tagging(Bucket=BUCKET_NAME, Key=TEST_KEY)["TagSet"]

        expected_body = b'{ "test": "object" }'
        expected_tags = [{"Key": "tag1", "Value": "test1"}, {"Key": "tag2", "Value": "test2"}]

        assert objectBody == expected_body
        assert object_tags == expected_tags


class TestS3Reader(unittest.TestCase):
    """Test suite for the S3 client reader"""

    @classmethod
    def setUpClass(cls) -> None:
        cls.s3_client: S3Client = boto3.client("s3")  # type: ignore
        cls.bucket = "my-bucket"
        cls.key = "my-key"

    def test_error_caught_if_read_fails(self) -> None:
        """Tests that a ClientError is raised if read fails"""

        reader = S3Reader(self.s3_client)
        fake_error = ClientError(
            operation_name="InvalidKeyPair.Duplicate",
            error_response={"Error": {"Code": "Duplicate", "Message": "This is a custom message"}},
        )
        reader._connection.get_object = MagicMock(side_effect=fake_error)

        with self.assertRaises((RuntimeError, ClientError)):
            reader.read(self.bucket, self.key)

    def test_get_request_made(self) -> None:
        """Test that the get request is made to s3 client"""

        reader = S3Reader(self.s3_client)
        reader._connection = MagicMock()

        reader.read(self.bucket, self.key)

        reader._connection.get_object.assert_called_once_with(Bucket=self.bucket, Key=self.key)
