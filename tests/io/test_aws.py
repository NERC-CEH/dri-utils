from typing import Any
from unittest.mock import MagicMock

import boto3
import moto
import pytest
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from mypy_boto3_s3.client import S3Client

from driutils.io.aws import S3Reader, S3Writer

BUCKET_NAME = "test_bucket"
TEST_KEY = "test_key"
TEST_OBJECT = b'{ "test": "object" }'
TEST_TAGS = {"tag1": "test1", "tag2": "test2"}


@moto.mock_aws
class TestS3Writer:
    def setup_s3_client(self) -> None:
        s3_client: S3Client = boto3.client("s3")  # type: ignore
        s3_client.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        return s3_client

    def test_s3_client_type(self) -> None:
        """Returns an object if s3_client is of type `boto3.client.s3`, otherwise
        raises an error"""
        s3_client = self.setup_s3_client()
        # Happy path
        writer = S3Writer(s3_client)

        assert isinstance(writer._connection, BaseClient)

        # Bad path
        with pytest.raises(TypeError):
            S3Writer("not an s3 client")  # type: ignore

    @pytest.mark.parametrize("body", [1, "body", 1.123, {"key": b"bytes"}])
    def test_error_raises_if_write_without_bytes(self, body: Any) -> None:
        """Tests that a type error is raised if the wrong type body used"""
        s3_client = self.setup_s3_client()
        writer = S3Writer(s3_client)
        writer._connection = MagicMock()

        with pytest.raises(TypeError):
            writer.write(BUCKET_NAME, TEST_KEY, body)

        writer._connection.put_object.assert_not_called()

    def test_write_called(self) -> None:
        """Tests that the writer can be executed"""
        s3_client = self.setup_s3_client()
        writer = S3Writer(s3_client)
        writer._connection = MagicMock()
        writer.write(BUCKET_NAME, TEST_KEY, TEST_OBJECT)

        writer._connection.put_object.assert_called_once_with(Bucket=BUCKET_NAME, Key=TEST_KEY, Body=TEST_OBJECT)

    def test_object_written(self) -> None:
        """Test the object is written correctly"""
        s3_client = self.setup_s3_client()
        writer = S3Writer(s3_client)

        writer.write(BUCKET_NAME, TEST_KEY, TEST_OBJECT, TEST_TAGS)
        objectBody = s3_client.get_object(Bucket=BUCKET_NAME, Key=TEST_KEY)["Body"].read()
        object_tags = s3_client.get_object_tagging(Bucket=BUCKET_NAME, Key=TEST_KEY)["TagSet"]

        expected_body = b'{ "test": "object" }'
        expected_tags = [{"Key": "tag1", "Value": "test1"}, {"Key": "tag2", "Value": "test2"}]

        assert objectBody == expected_body
        assert object_tags == expected_tags


class TestS3Reader:
    """Test suite for the S3 client reader"""

    bucket = "my-bucket"
    key = "my-key"

    @classmethod
    def setup_s3_client(cls) -> None:
        s3_client: S3Client = boto3.client("s3")  # type: ignore
        return s3_client

    def test_error_caught_if_read_fails(self) -> None:
        """Tests that a ClientError is raised if read fails"""
        s3_client = self.setup_s3_client()

        reader = S3Reader(s3_client)

        fake_error = ClientError(
            operation_name="InvalidKeyPair.Duplicate",
            error_response={"Error": {"Code": "Duplicate", "Message": "This is a custom message"}},
        )
        reader._connection.get_object = MagicMock(side_effect=fake_error)

        with pytest.raises((RuntimeError, ClientError)):
            reader.read(self.bucket, self.key)

    def test_get_request_made(self) -> None:
        """Test that the get request is made to s3 client"""
        s3_client = self.setup_s3_client()

        reader = S3Reader(s3_client)
        reader._connection = MagicMock()

        reader.read(self.bucket, self.key)

        reader._connection.get_object.assert_called_once_with(Bucket=self.bucket, Key=self.key)
