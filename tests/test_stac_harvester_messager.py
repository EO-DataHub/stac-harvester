import json
from unittest import mock

from eodhp_utils.messagers import Messager
from munch import DefaultMunch
from pytest_mock import MockerFixture

from stac_harvester.stac_harvester_messager import STACHarvesterMessager


def test_process_msg_updated(mocker: MockerFixture):
    mocker.patch(
        "stac_harvester.stac_harvester_messager.requests.get",
        return_value=DefaultMunch(json=lambda: {"type": "Feature", "id": "test-item"}),
    )

    mock_s3_client = mock.MagicMock()
    mock_producer = mock.MagicMock()

    test_stac_harvester = STACHarvesterMessager(
        s3_client=mock_s3_client,
        output_bucket="files_bucket_name",
        cat_output_prefix="stac-harvester",
        producer=mock_producer,
    )

    test_msg = {
        "files": [
            {
                "updated": True,
                "url": "https://test-url-to-catalog/collections/collection/items/item",
            }
        ],
        "source": "https://test-url-to-catalog",
        "target": "test-catalog",
    }

    expected_action = Messager.OutputFileAction(
        file_body=json.dumps({"type": "Feature", "id": "test-item"}),
        cat_path="supported-datasets/test-catalog/collections/collection/items/item",
    )

    result = test_stac_harvester.process_msg(test_msg)

    assert result == [expected_action]


def test_process_msg_deleted(mocker: MockerFixture):
    mocker.patch("stac_harvester.stac_harvester_messager.requests.get", return_value=None)

    mock_s3_client = mock.MagicMock()
    mock_producer = mock.MagicMock()

    test_stac_harvester = STACHarvesterMessager(
        s3_client=mock_s3_client,
        output_bucket="files_bucket_name",
        cat_output_prefix="stac-harvester",
        producer=mock_producer,
    )

    test_msg = {
        "files": [
            {
                "updated": False,
                "url": "https://test-url-to-catalog/collections/collection/items/item",
            }
        ],
        "source": "https://test-url-to-catalog",
        "target": "test-catalog",
    }

    expected_action = Messager.OutputFileAction(
        file_body=None,
        cat_path="supported-datasets/test-catalog/collections/collection/items/item",
    )

    result = test_stac_harvester.process_msg(test_msg)

    assert result == [expected_action]


def test_gen_empty_catalogue_message():

    mock_s3_client = mock.MagicMock()
    mock_producer = mock.MagicMock()

    test_stac_harvester = STACHarvesterMessager(
        s3_client=mock_s3_client,
        output_bucket="files_bucket_name",
        cat_output_prefix="stac-harvester",
        producer=mock_producer,
    )

    test_msg = {
        "files": [
            {
                "updated": True,
                "url": "https://test-url-to-catalog/collections/collection/items/item",
            }
        ],
        "source": "https://test-url-to-catalog",
        "target": "test-catalog",
    }

    result = test_stac_harvester.gen_empty_catalogue_message(test_msg)

    assert result == {
        "id": "stac-harvester/https://test-url-to-catalog",
        "workspace": "default_workspace",
        "repository": "",
        "branch": "",
        "bucket_name": "files_bucket_name",
        "source": "https://test-url-to-catalog",
        "target": "test-catalog",
    }
