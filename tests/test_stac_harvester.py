import json
import os
import sys
import tempfile
from unittest import mock

import boto3
import moto
import pytest
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
        "bucket_name": "test_bucket",
        "url": "https://test-url.co.uk/collection",
        "previously_harvested": {"https://test-url.co.uk/collection": "hash"},
    }


def test_generate_target_no_slash():
    test_catalog = "catalog"
    test_source_url = "https://www.this-is-a-stac-url.com"

    result = stac_harvester.generate_target(test_catalog, test_source_url)

    assert result == test_catalog


def test_generate_target_slash():
    test_catalog = "catalog"
    test_source_url = "https://www.this-is-a-stac-url.com/"

    result = stac_harvester.generate_target(test_catalog, test_source_url)

    assert result == test_catalog + "/"


def test_find_urls():
    """Test that find_urls only returns urls from valid links within a given input string"""
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
        result = stac_harvester.find_urls("https://www.this-is-a-stac-url.com")

    assert len(result) == 2
    assert f"{source_url}/link_1" in result


@moto.mock_aws()
def test_main(parameters, mocker: MockerFixture):
    """Test that the main stac havester function produces a correct pulsar message and updates
    known metadata in S3"""

    with moto.mock_aws():
        # Create S3 resources
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=parameters["bucket_name"])
        s3_resource = boto3.resource("s3")

        # Run STAC harvester
        sys.argv = [
            None,
            parameters["url"],
            "workspace-name",
            "catalog-name",
            parameters["bucket_name"],
        ]
        mocker.patch("stac_harvester.__main__.get_file_hash", return_value="hash")
        mocker.patch(
            "stac_harvester.__main__.find_urls", return_value=["https://test-url.co.uk/collection"]
        )
        pulsar_producer_mock = mocker.patch("stac_harvester.__main__.PulsarClient.create_producer")
        stac_harvester.main()

        # Verify output. URL is new and therefore in the "added" list
        s3_files = list(s3_resource.Bucket(parameters["bucket_name"]).objects.all())
        pulsar_message = pulsar_producer_mock.mock_calls[-1].args[0]
        pulsar_json = json.loads(pulsar_message.decode("utf-8"))
        assert pulsar_json["added_keys"] == [parameters["url"]]
        assert pulsar_json["updated_keys"] == []
        assert pulsar_json["deleted_keys"] == []
        assert len(s3_files) == 1


def test_main_with_previous_run(parameters, mocker: MockerFixture):
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
        s3.create_bucket(Bucket=parameters["bucket_name"])
        s3_resource = boto3.resource("s3")
        s3.upload_file(
            local_file_path,
            parameters["bucket_name"],
            f"harvested-metadata/{parameters['url'].replace('/', '')}",
        )

        # Run STAC harvester
        sys.argv = [
            None,
            parameters["url"],
            "workspace-name",
            "catalog-name",
            parameters["bucket_name"],
        ]
        mocker.patch("stac_harvester.__main__.get_file_hash", return_value="newhash")
        mocker.patch(
            "stac_harvester.__main__.find_urls", return_value=["https://test-url.co.uk/collection"]
        )
        pulsar_producer_mock = mocker.patch("stac_harvester.__main__.PulsarClient.create_producer")
        stac_harvester.main()

        # Verify output. URL already existed but the hash is different. URL is in the "updated" list
        s3_files = list(s3_resource.Bucket(parameters["bucket_name"]).objects.all())
        pulsar_message = pulsar_producer_mock.mock_calls[-1].args[0]
        pulsar_json = json.loads(pulsar_message.decode("utf-8"))
        assert pulsar_json["added_keys"] == []
        assert pulsar_json["updated_keys"] == [parameters["url"]]
        assert pulsar_json["deleted_keys"] == []
        assert len(s3_files) == 1
