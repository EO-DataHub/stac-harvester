import json
import os
import sys
import tempfile
from unittest import mock
from unittest.mock import MagicMock

import boto3
import moto
import pytest
from botocore.client import BaseClient
from munch import DefaultMunch
from pytest_mock import MockerFixture

import stac_harvester.__main__ as stac_harvester


@pytest.fixture(autouse=True)
def setenvvar(monkeypatch):
    with mock.patch.dict(os.environ, clear=True):
        envvars = {
            "PULSAR_URL": "https://mock.com",
        }
        for k, v in envvars.items():
            monkeypatch.setenv(k, v)

        yield


@pytest.fixture
def parameters() -> dict:
    """Parameters used by stac harvester tests"""
    return {
        "metadata_bucket_name": "test_metadata_bucket",
        "url": "https://test-url.co.uk/collection",
        "previously_harvested": {"https://test-url.co.uk/collection": "hash"},
    }


@pytest.fixture
def mock_catalogue_entry():
    entry = mock.Mock()
    entry.get_self_href.return_value = "https://test-url.co.uk/collection_new"

    return entry


@pytest.fixture
def mock_data():
    return {"id": "catalogue_name"}


def test_prepare_catalogue():
    """Test that prepare_catalogue returns a generator"""
    source_url = "https://www.this-is-a-stac-url.com"
    mock_catalogue_data = {
        "links": [
            {"rel": "child", "target": f"{source_url}/link_1"},
            {"rel": "new_rel", "target": f"{source_url}/invalid_rel"},
            {"rel": "child", "target": f"{source_url}/?invalid_link"},
            {"rel": "child"},
            {"rel": "child", "target": f"{source_url}/link_1"},
        ],
    }

    mock_catalogue = DefaultMunch.fromDict(mock_catalogue_data)

    mock_client = mock.MagicMock()
    mock_client.open.return_value.get_all_items.return_value = [mock_catalogue]

    with mock.patch("stac_harvester.__main__.PystacClient", mock_client):
        _, result = stac_harvester.prepare_catalogue_stac("https://www.this-is-a-stac-url.com")

    assert f"{source_url}/link_1" in str(list(result))


@moto.mock_aws()
def test_main(parameters, mocker: MockerFixture):
    """Test that the main stac havester function produces a correct pulsar message and updates
    known metadata in S3"""

    with moto.mock_aws():
        # Create S3 resources
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=parameters["metadata_bucket_name"])
        s3_resource = boto3.resource("s3")

        mock_catalogue_entry = mock.MagicMock()
        mock_catalogue_entry.get_self_href.return_value = "https://example.com"

        # Run STAC harvester
        sys.argv = [
            None,
            parameters["url"],
            "workspace-name",
            "catalog-name",
            "catalogue_target",
            parameters["metadata_bucket_name"],
        ]
        mocker.patch("stac_harvester.__main__.get_file_hash", return_value="hash")
        mocker.patch(
            "stac_harvester.__main__.prepare_catalogue_stac",
            return_value=(
                "catalogue_name",
                [
                    mock_catalogue_entry,
                ],
            ),
        )

        mock_pulsar_client = mock.MagicMock()
        mock_producer = mock.MagicMock()
        mock_pulsar_client.create_producer.return_value = mock_producer
        mocker.patch("stac_harvester.__main__.get_pulsar_client", return_value=mock_pulsar_client)

        with (
            mock.patch("stac_harvester.__main__.STACHarvesterMessager") as mock_messager,
            mock.patch("stac_harvester.__main__.parse_url_stac") as mock_stac_data,
        ):
            mock_stac_data.return_value = ("key", {"file content": "data"}, "hash")

            mock_messager_instance = mock_messager.return_value
            mock_messager_instance.consume.return_value = None
            stac_harvester.main()
            test_msg = {
                "source": "catalog-name",
                "target": "catalogue_target",
                "harvested_data": {"key.json": {"file content": "data"}},
                "deleted_keys": [],
            }

            # Verify messager created correctly
            mock_messager.assert_called_with(
                s3_client=mock.ANY,
                output_bucket=parameters["metadata_bucket_name"],
                cat_output_prefix="stac-harvester/catalog-name/",
                producer=mock_producer,
            )
            assert isinstance(mock_messager.call_args[1]["s3_client"], BaseClient)
            # Verify that the message is consumed by the messager
            mock_messager_instance.consume.assert_called_with(test_msg)

        # Verify that hashed data is uploaded to S3
        s3_files = list(s3_resource.Bucket(parameters["metadata_bucket_name"]).objects.all())
        assert len(s3_files) == 1


def test_main_with_previous_run_updated(parameters, mocker: MockerFixture, mock_catalogue_entry):
    """Test that the main stac havester function produces a correct pulsar message and updates known
    metadata in S3, when given known metadata from a previous run"""

    with moto.mock_aws(), tempfile.TemporaryDirectory() as temp_dir:
        # Create local file imitating stored harvested files from a previous run
        harvested_file_name = "harvested.txt"
        folder_path = f"{temp_dir}/test"
        os.makedirs(folder_path)
        local_file_path = f"{folder_path}/{harvested_file_name}"
        with open(local_file_path, "w") as temp_file:
            temp_file.write(json.dumps(parameters["previously_harvested"]))
            temp_file.flush()

        # Create S3 resources and upload file to S3
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=parameters["metadata_bucket_name"])
        s3_resource = boto3.resource("s3")
        s3.upload_file(
            local_file_path,
            parameters["metadata_bucket_name"],
            f"harvested-metadata/{parameters['url'].replace('/', '')}",
        )

        # Run STAC harvester
        sys.argv = [
            None,
            parameters["url"],
            "workspace-name",
            "catalog-name",
            "catalogue_target",
            parameters["metadata_bucket_name"],
        ]
        mocker.patch("stac_harvester.__main__.get_file_hash", return_value="newhash")
        mocker.patch(
            "stac_harvester.__main__.prepare_catalogue_stac",
            return_value=("catalogue", [mock_catalogue_entry]),
        )
        mock_pulsar_client = mock.MagicMock()
        mock_producer = mock.MagicMock()
        mock_pulsar_client.create_producer.return_value = mock_producer
        mocker.patch("stac_harvester.__main__.get_pulsar_client", return_value=mock_pulsar_client)

        with (
            mock.patch("stac_harvester.__main__.STACHarvesterMessager") as mock_messager,
            mock.patch("stac_harvester.__main__.get_file_contents") as mock_file_contents,
        ):

            mock_file_contents.return_value = (
                '{"id": "test_catalogue", "type": "Catalog", "links": []}'.encode("utf-8")
            )

            mock_messager_instance = mock_messager.return_value
            mock_messager_instance.consume.return_value = None
            stac_harvester.main()
            test_msg = {
                "source": "catalog-name",
                "target": "catalogue_target",
                "harvested_data": {
                    "catalogs/test_catalogue.json": {
                        "id": "test_catalogue",
                        "type": "Catalog",
                        "links": [],
                    }
                },
                "deleted_keys": ["https://test-url.co.uk/collection"],
            }
            # Verify messager created correctly
            mock_messager.assert_called_with(
                s3_client=mock.ANY,
                output_bucket=parameters["metadata_bucket_name"],
                cat_output_prefix="stac-harvester/catalog-name/",
                producer=mock_producer,
            )
            assert isinstance(mock_messager.call_args[1]["s3_client"], BaseClient)
            # Verify that the message is consumed by the messager
            mock_messager_instance.consume.assert_called_with(test_msg)

        # Verify that hashed data is uploaded to S3
        s3_files = list(s3_resource.Bucket(parameters["metadata_bucket_name"]).objects.all())
        assert len(s3_files) == 1


def test_main_with_previous_run_deleted(
    parameters, mocker: MockerFixture, mock_catalogue_entry, mock_data
):
    """Test that the main stac havester function produces a correct pulsar message and updates known
    metadata in S3, when given known metadata from a previous run"""

    with moto.mock_aws(), tempfile.TemporaryDirectory() as temp_dir:
        # Create local file imitating stored harvested files from a previous run
        harvested_file_name = "harvested.txt"
        folder_path = f"{temp_dir}/test"
        os.makedirs(folder_path)
        local_file_path = f"{folder_path}/{harvested_file_name}"
        with open(local_file_path, "w") as temp_file:
            temp_file.write(json.dumps(parameters["previously_harvested"]))
            temp_file.flush()

        # Create S3 resources and upload file to S3
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=parameters["metadata_bucket_name"])
        s3_resource = boto3.resource("s3")
        s3.upload_file(
            local_file_path,
            parameters["metadata_bucket_name"],
            f"harvested-metadata/{parameters['url'].replace('/', '')}",
        )

        # Run STAC harvester
        sys.argv = [
            None,
            parameters["url"],
            "workspace-name",
            "catalog-name",
            "catalog-target",
            parameters["metadata_bucket_name"],
        ]
        mocker.patch("stac_harvester.__main__.get_file_hash", return_value="newhash")
        mocker.patch(
            "stac_harvester.__main__.prepare_catalogue_stac",
            return_value=("catalogue_name", [mock_catalogue_entry]),
        )
        mock_pulsar_client = mock.MagicMock()
        mock_producer = mock.MagicMock()
        mock_pulsar_client.create_producer.return_value = mock_producer
        mocker.patch("stac_harvester.__main__.get_pulsar_client", return_value=mock_pulsar_client)

        with (
            mock.patch("stac_harvester.__main__.STACHarvesterMessager") as mock_messager,
            mock.patch("stac_harvester.__main__.get_file_contents") as mock_file_contents,
        ):

            mock_file_contents.return_value = (
                '{"id": "test_catalogue", "type": "Catalog", "links": []}'.encode("utf-8")
            )

            mock_messager_instance = mock_messager.return_value
            mock_messager_instance.consume.return_value = None
            stac_harvester.main()
            test_msg = {
                "source": "catalog-name",
                "target": "catalog-target",
                "harvested_data": {
                    "catalogs/test_catalogue.json": {
                        "id": "test_catalogue",
                        "type": "Catalog",
                        "links": [],
                    }
                },
                "deleted_keys": ["https://test-url.co.uk/collection"],
            }
            # Verify messager created correctly
            mock_messager.assert_called_once_with(
                s3_client=mock.ANY,
                output_bucket=parameters["metadata_bucket_name"],
                cat_output_prefix="stac-harvester/catalog-name/",
                producer=mock_producer,
            )
            assert isinstance(mock_messager.call_args[1]["s3_client"], BaseClient)
            # Verify that the message is consumed by the messager
            mock_messager_instance.consume.assert_called_with(test_msg)

        # Verify that hashed data is uploaded to S3
        s3_files = list(s3_resource.Bucket(parameters["metadata_bucket_name"]).objects.all())

        assert len(s3_files) == 1
