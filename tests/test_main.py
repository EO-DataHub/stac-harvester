import http.client
import json
import os
import sys
import tempfile
from collections.abc import Generator
from typing import Any
from unittest import mock

import boto3
import moto
import pytest
from botocore.client import BaseClient
from munch import DefaultMunch
from pytest_mock import MockerFixture
from urllib3.util.retry import Retry

import stac_harvester.__main__ as stac_harvester


@pytest.fixture(autouse=True)
def setenvvar(monkeypatch: pytest.MonkeyPatch) -> Generator[None]:
    with mock.patch.dict(os.environ, clear=True):
        envvars = {
            "PULSAR_URL": "https://mock.com",
        }
        for k, v in envvars.items():
            monkeypatch.setenv(k, v)

        yield


@pytest.fixture
def parameters() -> dict[str, Any]:
    """Parameters used by stac harvester tests"""
    return {
        "metadata_bucket_name": "test_metadata_bucket",
        "url": "https://test-url.co.uk/collection",
        "previously_harvested": {"https://test-url.co.uk/collection": "hash"},
    }


@pytest.fixture
def mock_catalogue_entry() -> mock.Mock:
    entry = mock.Mock()
    entry.get_self_href.return_value = "https://test-url.co.uk/collection_new"

    return entry


@pytest.fixture
def mock_data() -> dict[str, str]:
    return {"id": "catalogue_name"}


def test_prepare_catalogue() -> None:
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
    mock_client.id = "test-catalogue"
    mock_client.get_root.return_value = mock_catalogue
    mock_client.get_collections.return_value = []
    mock_search = mock.MagicMock()
    mock_search.items_as_dicts.return_value = [
        {"type": "Feature", "id": "item1", "links": [{"rel": "self", "href": f"{source_url}/link_1"}]},
    ]
    mock_client.search.return_value = mock_search

    with mock.patch("stac_harvester.__main__._create_stac_client", return_value=mock_client):
        _, result, _ = stac_harvester.prepare_catalogue_stac("https://www.this-is-a-stac-url.com")

    result_list = list(result)
    assert any(f"{source_url}/link_1" in str(item) for item in result_list), (
        f"Expected link_1 in result, got: {result_list}"
    )


@moto.mock_aws()
def test_main(parameters: dict[str, Any], mocker: MockerFixture) -> None:
    """Test that the main stac havester function produces a correct pulsar message and updates
    known metadata in S3"""

    with moto.mock_aws():
        # Create S3 resources
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=parameters["metadata_bucket_name"])
        s3_resource: Any = boto3.resource("s3")

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
            return_value=("catalogue_name", [mock_catalogue_entry], "https://example.com"),
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


def test_main_with_previous_run_updated(
    parameters: dict[str, Any], mocker: MockerFixture, mock_catalogue_entry: mock.Mock
) -> None:
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
        s3_resource: Any = boto3.resource("s3")
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
            return_value=("catalogue", [mock_catalogue_entry], "https://example.com"),
        )
        mock_pulsar_client = mock.MagicMock()
        mock_producer = mock.MagicMock()
        mock_pulsar_client.create_producer.return_value = mock_producer
        mocker.patch("stac_harvester.__main__.get_pulsar_client", return_value=mock_pulsar_client)

        with (
            mock.patch("stac_harvester.__main__.STACHarvesterMessager") as mock_messager,
            mock.patch("stac_harvester.__main__.get_file_contents") as mock_file_contents,
        ):
            mock_file_contents.return_value = b'{"id": "test_catalogue", "type": "Catalog", "links": []}'

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
    parameters: dict[str, Any],
    mocker: MockerFixture,
    mock_catalogue_entry: mock.Mock,
    mock_data: dict[str, str],
) -> None:
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
        s3_resource: Any = boto3.resource("s3")
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
            return_value=("catalogue_name", [mock_catalogue_entry], "https://example.com"),
        )
        mock_pulsar_client = mock.MagicMock()
        mock_producer = mock.MagicMock()
        mock_pulsar_client.create_producer.return_value = mock_producer
        mocker.patch("stac_harvester.__main__.get_pulsar_client", return_value=mock_pulsar_client)

        with (
            mock.patch("stac_harvester.__main__.STACHarvesterMessager") as mock_messager,
            mock.patch("stac_harvester.__main__.get_file_contents") as mock_file_contents,
        ):
            mock_file_contents.return_value = b'{"id": "test_catalogue", "type": "Catalog", "links": []}'

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


@moto.mock_aws()
def test_main_skips_bad_item_and_continues(parameters: dict[str, Any], mocker: MockerFixture) -> None:
    """Test that a per-item exception (e.g. InvalidURL) is caught and the loop continues
    to process subsequent items."""
    with moto.mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=parameters["metadata_bucket_name"])

        # Two mock items: first will cause parse_url_stac to raise, second succeeds
        bad_item = mock.MagicMock()
        bad_item.get_self_href.return_value = "https://example.com/bad item"

        good_item = mock.MagicMock()
        good_item.get_self_href.return_value = "https://example.com/good-item"

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
            return_value=("catalogue_name", [bad_item, good_item], "https://example.com"),
        )

        mock_pulsar_client = mock.MagicMock()
        mock_producer = mock.MagicMock()
        mock_pulsar_client.create_producer.return_value = mock_producer
        mocker.patch("stac_harvester.__main__.get_pulsar_client", return_value=mock_pulsar_client)

        with (
            mock.patch("stac_harvester.__main__.STACHarvesterMessager") as mock_messager,
            mock.patch("stac_harvester.__main__.parse_url_stac") as mock_parse,
        ):

            def parse_side_effect(url: str) -> tuple[str, dict[str, Any], str]:
                if "bad" in url:
                    raise http.client.InvalidURL(f"URL contains illegal character: {url!r}")
                return ("good-key", {"file content": "good data"}, "hash")

            mock_parse.side_effect = parse_side_effect

            mock_messager_instance = mock_messager.return_value
            mock_messager_instance.consume.return_value = None

            stac_harvester.main()

            # The good item should appear in the harvested data
            final_call_args = mock_messager_instance.consume.call_args[0][0]
            assert "good-key.json" in final_call_args["harvested_data"]
            assert final_call_args["harvested_data"]["good-key.json"] == {"file content": "good data"}

            # consume was called, proving the harvest completed
            assert mock_messager_instance.consume.called


def test_create_stac_client_has_post_retry() -> None:
    """Test that _create_stac_client configures retry with POST support."""
    with (
        mock.patch("stac_harvester.__main__.PystacClient") as mock_pystac,
        mock.patch("stac_harvester.__main__.StacApiIO") as mock_stac_io,
    ):
        stac_harvester._create_stac_client("https://example.com")

        # Verify StacApiIO was called with a Retry that allows all methods
        mock_stac_io.assert_called_once()
        retry_arg = mock_stac_io.call_args[1]["max_retries"]
        assert isinstance(retry_arg, Retry)
        assert retry_arg.total == 5
        assert retry_arg.allowed_methods is None
        assert retry_arg.backoff_factor == 1
        assert 502 in retry_arg.status_forcelist
        assert 503 in retry_arg.status_forcelist
        assert 429 in retry_arg.status_forcelist
        assert 504 in retry_arg.status_forcelist

        # Verify PystacClient.open was called with the StacApiIO instance
        mock_pystac.open.assert_called_once_with("https://example.com", stac_io=mock_stac_io.return_value)


def test_get_item_self_href_from_dict() -> None:
    """Test that _get_item_self_href extracts self link from item dicts."""
    item_dict = {
        "type": "Feature",
        "id": "test-item",
        "links": [
            {"rel": "self", "href": "https://example.com/item/123"},
            {"rel": "parent", "href": "https://example.com/collection"},
        ],
    }
    assert stac_harvester._get_item_self_href(item_dict) == "https://example.com/item/123"


def test_get_item_self_href_from_dict_no_self_link() -> None:
    """Test that _get_item_self_href returns None when no self link exists."""
    item_dict = {
        "type": "Feature",
        "id": "test-item",
        "links": [{"rel": "parent", "href": "https://example.com/collection"}],
    }
    assert stac_harvester._get_item_self_href(item_dict) is None


def test_get_item_self_href_resolves_relative_href() -> None:
    """Test that _get_item_self_href resolves relative self links against base_url."""
    item_dict = {
        "type": "Feature",
        "id": "test-item",
        "links": [{"rel": "self", "href": "items/123"}],
    }
    assert (
        stac_harvester._get_item_self_href(item_dict, base_url="https://example.com/collections/col1")
        == "https://example.com/collections/col1/items/123"
    )


def test_get_item_self_href_resolves_relative_href_with_trailing_slash() -> None:
    """Test that _get_item_self_href resolves relative links correctly
    when base_url has no trailing slash (path segment is preserved, not dropped)."""
    item_dict = {
        "type": "Feature",
        "id": "test-item",
        "links": [{"rel": "self", "href": "items/123"}],
    }
    assert (
        stac_harvester._get_item_self_href(item_dict, base_url="https://example.com/api/stac/v1")
        == "https://example.com/api/stac/v1/items/123"
    )


def test_get_item_self_href_absolute_href_unchanged() -> None:
    """Test that _get_item_self_href doesn't modify absolute hrefs even with base_url."""
    item_dict = {
        "type": "Feature",
        "id": "test-item",
        "links": [{"rel": "self", "href": "https://other.com/item/123"}],
    }
    assert (
        stac_harvester._get_item_self_href(item_dict, base_url="https://example.com/collections/col1")
        == "https://other.com/item/123"
    )


def test_get_item_self_href_from_pystac_item() -> None:
    """Test that _get_item_self_href delegates to get_self_href for pystac objects."""
    mock_item = mock.MagicMock()
    mock_item.get_self_href.return_value = "https://example.com/item/456"
    assert stac_harvester._get_item_self_href(mock_item) == "https://example.com/item/456"


def test_get_item_self_href_from_pystac_item_error() -> None:
    """Test that _get_item_self_href returns None if get_self_href raises."""
    mock_item = mock.MagicMock()
    mock_item.get_self_href.side_effect = AttributeError("broken")
    assert stac_harvester._get_item_self_href(mock_item) is None


def test_prepare_catalogue_uses_items_as_dicts_when_supported() -> None:
    """Test that prepare_catalogue_stac uses items_as_dicts when ITEM_SEARCH is supported."""

    mock_client = mock.MagicMock()
    mock_client.id = "test-catalogue"
    mock_client.get_root.return_value = mock.MagicMock()
    mock_client.get_collections.return_value = []
    mock_client.conforms_to.return_value = True
    mock_client._search_href.return_value = "https://example.com/search"
    mock_search = mock.MagicMock()
    mock_search.items_as_dicts.return_value = [
        {"type": "Feature", "id": "item1", "links": [{"rel": "self", "href": "https://example.com/item1"}]},
    ]
    mock_client.search.return_value = mock_search

    with mock.patch("stac_harvester.__main__._create_stac_client", return_value=mock_client):
        _, result, base_url = stac_harvester.prepare_catalogue_stac("https://example.com")

    list(result)
    mock_client.search.assert_called_once()
    mock_search.items_as_dicts.assert_called_once()
    mock_client.get_all_items.assert_not_called()
    assert base_url is not None


def test_prepare_catalogue_falls_back_to_get_all_items_when_unsupported() -> None:
    """Test that prepare_catalogue_stac falls back to get_all_items when ITEM_SEARCH is unsupported."""

    mock_client = mock.MagicMock()
    mock_client.id = "test-catalogue"
    mock_client.get_root.return_value = mock.MagicMock()
    mock_client.get_collections.return_value = []
    mock_client.conforms_to.return_value = False
    mock_client.get_all_items.return_value = [
        mock.MagicMock(),
    ]

    with mock.patch("stac_harvester.__main__._create_stac_client", return_value=mock_client):
        _, result, _base_url = stac_harvester.prepare_catalogue_stac("https://example.com")

    list(result)
    mock_client.search.assert_not_called()
    mock_client.get_all_items.assert_called_once()


@moto.mock_aws()
def test_main_skips_item_dict_without_self_link_and_continues(
    parameters: dict[str, Any], mocker: MockerFixture
) -> None:
    """Test that an item dict missing a self link is skipped and the harvest continues.
    This simulates the scenario from GitHub issue #41 where a malformed STAC item
    would crash the entire harvest."""
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=parameters["metadata_bucket_name"])

    bad_item = {
        "type": "Feature",
        "id": "broken-item",
        "links": [{"rel": "parent", "href": "https://example.com/collection"}],
    }
    good_item = {
        "type": "Feature",
        "id": "good-item",
        "links": [{"rel": "self", "href": "https://example.com/good-item"}],
    }

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
        return_value=("catalogue_name", [bad_item, good_item], "https://example.com"),
    )

    mock_pulsar_client = mock.MagicMock()
    mock_producer = mock.MagicMock()
    mock_pulsar_client.create_producer.return_value = mock_producer
    mocker.patch("stac_harvester.__main__.get_pulsar_client", return_value=mock_pulsar_client)

    with (
        mock.patch("stac_harvester.__main__.STACHarvesterMessager") as mock_messager,
        mock.patch("stac_harvester.__main__._get_item_self_href") as mock_href,
        mock.patch("stac_harvester.__main__.parse_url_stac") as mock_parse,
    ):
        mock_href.side_effect = [None, "https://example.com/good-item"]
        mock_parse.return_value = ("good-key", {"file content": "good data"}, "hash")

        mock_messager_instance = mock_messager.return_value
        mock_messager_instance.consume.return_value = None
        stac_harvester.main()

        final_call_args = mock_messager_instance.consume.call_args[0][0]
        assert "good-key.json" in final_call_args["harvested_data"]


@moto.mock_aws()
def test_main_flushes_on_iterator_failure(parameters: dict[str, Any], mocker: MockerFixture) -> None:
    """Test that when the iterator itself raises (e.g. connection abort during pagination),
    already-harvested data is still flushed and no items are reported as deleted."""
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=parameters["metadata_bucket_name"])

    good_item = mock.MagicMock()
    good_item.get_self_href.return_value = "https://example.com/good-item"

    def failing_iterator() -> Generator[mock.MagicMock]:
        yield good_item
        raise ConnectionError("Remote end closed connection without response")

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
        return_value=("catalogue_name", failing_iterator(), "https://example.com"),
    )

    mock_pulsar_client = mock.MagicMock()
    mock_producer = mock.MagicMock()
    mock_pulsar_client.create_producer.return_value = mock_producer
    mocker.patch("stac_harvester.__main__.get_pulsar_client", return_value=mock_pulsar_client)

    with (
        mock.patch("stac_harvester.__main__.STACHarvesterMessager") as mock_messager,
        mock.patch("stac_harvester.__main__.parse_url_stac") as mock_parse,
    ):
        mock_parse.return_value = ("good-key", {"file content": "good data"}, "hash")
        mock_messager_instance = mock_messager.return_value
        mock_messager_instance.consume.return_value = None

        stac_harvester.main()

        # The good item harvested before the failure should still be flushed
        consume_calls = mock_messager_instance.consume.call_args_list
        all_harvested = {}
        for call in consume_calls:
            all_harvested.update(call[0][0].get("harvested_data", {}))
        assert "good-key.json" in all_harvested

        # No items should be reported as deleted when iterator fails
        all_deleted = []
        for call in consume_calls:
            all_deleted.extend(call[0][0].get("deleted_keys", []))
        assert all_deleted == [], f"Expected no deletions on iterator failure, got: {all_deleted}"


@moto.mock_aws()
def test_main_flushes_on_attribute_error_from_iterator(parameters: dict[str, Any], mocker: MockerFixture) -> None:
    """Test the exact scenario from GitHub issue #41: the iterator raises
    AttributeError (e.g. from pystac Item.from_dict on malformed items).
    The harvest should not crash and should flush already-harvested data."""
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=parameters["metadata_bucket_name"])

    good_item = mock.MagicMock()
    good_item.get_self_href.return_value = "https://example.com/good-item"

    def failing_iterator() -> Generator[mock.MagicMock]:
        yield good_item
        raise AttributeError("'list' object has no attribute 'items'")

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
        return_value=("catalogue_name", failing_iterator(), "https://example.com"),
    )

    mock_pulsar_client = mock.MagicMock()
    mock_producer = mock.MagicMock()
    mock_pulsar_client.create_producer.return_value = mock_producer
    mocker.patch("stac_harvester.__main__.get_pulsar_client", return_value=mock_pulsar_client)

    with (
        mock.patch("stac_harvester.__main__.STACHarvesterMessager") as mock_messager,
        mock.patch("stac_harvester.__main__.parse_url_stac") as mock_parse,
    ):
        mock_parse.return_value = ("good-key", {"file content": "good data"}, "hash")
        mock_messager_instance = mock_messager.return_value
        mock_messager_instance.consume.return_value = None

        stac_harvester.main()

        consume_calls = mock_messager_instance.consume.call_args_list
        all_harvested = {}
        for call in consume_calls:
            all_harvested.update(call[0][0].get("harvested_data", {}))
        assert "good-key.json" in all_harvested

        all_deleted = []
        for call in consume_calls:
            all_deleted.extend(call[0][0].get("deleted_keys", []))
        assert all_deleted == []
