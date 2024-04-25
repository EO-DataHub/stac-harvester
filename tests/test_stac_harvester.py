import json
import os
import sys
import tempfile

import boto3
import moto
import pytest
from pytest_mock import MockerFixture

import stac_harvester.__main__ as stac_harvester


@pytest.fixture
def parameters() -> dict:
    """Parameters used by stac harvester tests"""
    return {
        "bucket_name": "test_bucket",
        "url": "https://test-url.co.uk/collection",
        "previously_harvested": {"https://test-url.co.uk/collection": "hash"},
    }


def test_find_urls():
    """Test that find_urls only returns urls from valid links within a given input string"""
    input = {
        "links": [
            {"rel": "child", "href": "link_1"},
            {"rel": "parent", "href": "invalid_rel"},
            {"rel": "child", "href": "?invalid_link"},
            {"rel": "child"},
            {"rel": "child", "href": "invalid_duplicate"},
            {"rel": "child", "href": "link_1"},
        ],
        "subcategory": {"links": [{"rel": "child", "href": "link_2"}]},
    }

    result = stac_harvester.find_urls(json.dumps(input), "", ["invalid_duplicate"], [])

    assert len(result) == 2
    assert "link_1" in result
    assert "link_2" in result


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
            "subcatalog-name",
            parameters["bucket_name"],
        ]
        mocker.patch("stac_harvester.__main__.get_file_contents", return_value=("test", "hash"))
        pulsar_producer_mock = mocker.patch("stac_harvester.__main__.Client.create_producer")
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
            "subcatalog-name",
            parameters["bucket_name"],
        ]
        mocker.patch("stac_harvester.__main__.get_file_contents", return_value=("test", "newhash"))
        pulsar_producer_mock = mocker.patch("stac_harvester.__main__.Client.create_producer")
        stac_harvester.main()

        # Verify output. URL already existed but the hash is different. URL is in the "updated" list
        s3_files = list(s3_resource.Bucket(parameters["bucket_name"]).objects.all())
        pulsar_message = pulsar_producer_mock.mock_calls[-1].args[0]
        pulsar_json = json.loads(pulsar_message.decode("utf-8"))
        assert pulsar_json["added_keys"] == []
        assert pulsar_json["updated_keys"] == [parameters["url"]]
        assert pulsar_json["deleted_keys"] == []
        assert len(s3_files) == 1
