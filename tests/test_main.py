import copy
import http.client
import json
import os
import sys
import tempfile
import urllib.error
from collections.abc import Callable, Generator, Iterable
from email.message import Message
from typing import Any
from unittest import mock

import boto3
import moto
import pytest
from botocore.client import BaseClient
from eodhp_utils.messagers import Messager
from munch import DefaultMunch
from pystac import Catalog, Collection, Item
from pystac.errors import STACError, STACTypeError
from pystac_client.errors import ClientTypeError
from pystac_client.exceptions import APIError
from pytest_mock import MockerFixture
from requests_mock import Mocker as RequestsMocker
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


@pytest.fixture(autouse=True)
def clear_parent_keys() -> None:
    """parent_keys is a module global, so it would otherwise leak between tests"""
    stac_harvester.parent_keys.clear()


@pytest.fixture
def parameters() -> dict[str, Any]:
    """Parameters used by stac harvester tests"""
    return {
        "metadata_bucket_name": "test_metadata_bucket",
        "url": "https://test-url.co.uk/collection",
        "previously_harvested": {"catalogs/test_catalogue/collections/c/items/old-item.json": "hash"},
    }


def _previous_run_file_contents(url: str) -> bytes:
    """One catalogue and one item that is still listed upstream"""
    if url.endswith("/kept-item"):
        return b'{"id": "kept-item", "type": "Feature", "links": []}'
    return b'{"id": "test_catalogue", "type": "Catalog", "links": []}'


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
    mock_client.get_root.return_value = mock.MagicMock()
    mock_client.get_collections.return_value = []
    mock_client.conforms_to.return_value = False
    mock_client.get_all_items.return_value = [mock_catalogue]

    with mock.patch("stac_harvester.__main__._create_stac_client", return_value=mock_client):
        _, result = stac_harvester.prepare_catalogue_stac("https://www.this-is-a-stac-url.com")

    assert f"{source_url}/link_1" in str(list(result))


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
            return_value=("catalogue", [mock_catalogue_entry, _entry("https://test-url.co.uk/kept-item")]),
        )
        mock_pulsar_client = mock.MagicMock()
        mock_producer = mock.MagicMock()
        mock_pulsar_client.create_producer.return_value = mock_producer
        mocker.patch("stac_harvester.__main__.get_pulsar_client", return_value=mock_pulsar_client)

        with (
            mock.patch("stac_harvester.__main__.STACHarvesterMessager") as mock_messager,
            mock.patch("stac_harvester.__main__.get_file_contents") as mock_file_contents,
        ):
            mock_file_contents.side_effect = _previous_run_file_contents

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
                    },
                    "items/kept-item.json": {"id": "kept-item", "type": "Feature", "links": []},
                },
                "deleted_keys": ["catalogs/test_catalogue/collections/c/items/old-item.json"],
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
            return_value=("catalogue_name", [mock_catalogue_entry, _entry("https://test-url.co.uk/kept-item")]),
        )
        mock_pulsar_client = mock.MagicMock()
        mock_producer = mock.MagicMock()
        mock_pulsar_client.create_producer.return_value = mock_producer
        mocker.patch("stac_harvester.__main__.get_pulsar_client", return_value=mock_pulsar_client)

        with (
            mock.patch("stac_harvester.__main__.STACHarvesterMessager") as mock_messager,
            mock.patch("stac_harvester.__main__.get_file_contents") as mock_file_contents,
        ):
            mock_file_contents.side_effect = _previous_run_file_contents

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
                    },
                    "items/kept-item.json": {"id": "kept-item", "type": "Feature", "links": []},
                },
                "deleted_keys": ["catalogs/test_catalogue/collections/c/items/old-item.json"],
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
def test_main_skips_bad_item_and_continues(
    parameters: dict[str, Any], mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that a per-item exception (e.g. InvalidURL) is caught and the loop continues
    to process subsequent items."""
    # Override the circuit breaker so that only partial-run handling can prevent the delete
    monkeypatch.setenv("ALLOW_UNSAFE_DELETES", "true")
    with moto.mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=parameters["metadata_bucket_name"])
        previous_metadata = {"collections/old/items/old-item.json": "old-hash"}
        metadata_key = f"harvested-metadata/{parameters['url'].replace('/', '')}"
        s3.put_object(
            Bucket=parameters["metadata_bucket_name"],
            Key=metadata_key,
            Body=json.dumps(previous_metadata),
        )

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
            return_value=("catalogue_name", [bad_item, good_item]),
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
                return ("collections/c/items/good-item", {"file content": "good data"}, "hash")

            mock_parse.side_effect = parse_side_effect

            mock_messager_instance = mock_messager.return_value
            mock_messager_instance.consume.return_value = None

            stac_harvester.main()

            # The good item should appear in the harvested data
            final_call_args = mock_messager_instance.consume.call_args[0][0]
            assert "collections/c/items/good-item.json" in final_call_args["harvested_data"]
            assert final_call_args["harvested_data"]["collections/c/items/good-item.json"] == {
                "file content": "good data"
            }

            # consume was called, proving the harvest completed
            assert mock_messager_instance.consume.called

            # A skipped item makes the run partial, so nothing may be deleted
            all_deleted = []
            for call in mock_messager_instance.consume.call_args_list:
                all_deleted.extend(call[0][0].get("deleted_keys", []))
            assert all_deleted == []

        # Previous metadata is merged with the newly harvested hashes
        stored_metadata = json.loads(
            s3.get_object(Bucket=parameters["metadata_bucket_name"], Key=metadata_key)["Body"].read().decode("utf-8")
        )
        assert stored_metadata == previous_metadata | {"collections/c/items/good-item.json": "hash"}


def test_create_stac_client_has_post_retry() -> None:
    """Test that _create_stac_client configures retry with POST support."""
    with (
        mock.patch("stac_harvester.__main__.PystacClient") as mock_pystac,
        mock.patch("stac_harvester.__main__.ListingValidatingStacApiIO") as mock_stac_io,
    ):
        stac_harvester._create_stac_client("https://example.com")

        # Verify StacApiIO was called with a Retry that allows all methods
        mock_stac_io.assert_called_once()
        retry_arg = mock_stac_io.call_args[1]["max_retries"]
        assert isinstance(retry_arg, Retry)
        assert retry_arg.total == 5
        assert retry_arg.allowed_methods is None
        assert retry_arg.backoff_factor == 1
        assert 500 in retry_arg.status_forcelist
        assert 502 in retry_arg.status_forcelist
        assert 503 in retry_arg.status_forcelist
        assert 429 in retry_arg.status_forcelist
        assert 504 in retry_arg.status_forcelist
        assert mock_stac_io.call_args[1]["timeout"] == stac_harvester.FETCH_TIMEOUT_SECONDS

        # Verify PystacClient.open was called with the StacApiIO instance
        mock_pystac.open.assert_called_once_with(
            "https://example.com", stac_io=mock_stac_io.return_value, timeout=stac_harvester.FETCH_TIMEOUT_SECONDS
        )


def test_get_item_id_uses_dict_id_fallback() -> None:
    """Malformed non-Git STAC item dicts should log their upstream item id."""
    assert stac_harvester.get_item_id({"id": "bad-upstream-item"}) == "bad-upstream-item"


def test_iter_search_items_safely_skips_malformed_item_and_continues() -> None:
    """Malformed item-search features should not stop later valid features being harvested."""
    client = mock.MagicMock()
    run_state = stac_harvester.HarvestRunState()
    search = mock.MagicMock()
    search.items_as_dicts.return_value = [
        {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "broken-item",
            "geometry": None,
            "bbox": None,
            "properties": {},
            "links": [{"rel": "self", "href": "https://example.com/items/broken-item"}],
        },
        {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "good-item",
            "geometry": None,
            "bbox": None,
            "properties": {"datetime": "2020-01-01T00:00:00Z"},
            "links": [{"rel": "self", "href": "https://example.com/items/good-item"}],
        },
    ]
    client.search.return_value = search

    items = list(stac_harvester.iter_search_items_safely(client, run_state))

    assert len(items) == 1
    assert items[0].id == "good-item"
    assert items[0].get_self_href() == "https://example.com/items/good-item"
    assert run_state.is_partial is True


def test_iter_search_items_safely_resolves_relative_self_href() -> None:
    """Relative self hrefs from item-search must not become local filesystem paths."""
    client = mock.MagicMock()
    search = mock.MagicMock()
    search.url = "https://example.com/search"
    search.items_as_dicts.return_value = [
        {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "relative-item",
            "geometry": None,
            "bbox": None,
            "properties": {"datetime": "2020-01-01T00:00:00Z"},
            "links": [{"rel": "self", "href": "items/relative-item"}],
        },
    ]
    client.search.return_value = search

    items = list(stac_harvester.iter_search_items_safely(client))

    assert len(items) == 1
    assert items[0].get_self_href() == "https://example.com/search/items/relative-item"


def test_iter_search_items_safely_skips_non_object_feature() -> None:
    """Malformed non-object item-search features should be skipped."""
    client = mock.MagicMock()
    run_state = stac_harvester.HarvestRunState()
    search = mock.MagicMock()
    search.url = "https://example.com/search"
    search.items_as_dicts.return_value = [
        None,
        {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "good-item",
            "geometry": None,
            "bbox": None,
            "properties": {"datetime": "2020-01-01T00:00:00Z"},
            "links": [{"rel": "self", "href": "https://example.com/items/good-item"}],
        },
    ]
    client.search.return_value = search

    items = list(stac_harvester.iter_search_items_safely(client, run_state))

    assert len(items) == 1
    assert items[0].id == "good-item"
    assert run_state.is_partial is True


def test_prepare_catalogue_uses_safe_search_iterator_when_item_search_supported() -> None:
    """Item-search APIs should use safe per-feature conversion, not raw get_all_items()."""
    mock_client = mock.MagicMock()
    mock_client.id = "test-catalogue"
    mock_client.get_root.return_value = mock.MagicMock()
    mock_client.get_collections.return_value = []
    mock_client.conforms_to.return_value = True

    with (
        mock.patch("stac_harvester.__main__._create_stac_client", return_value=mock_client),
        mock.patch("stac_harvester.__main__.iter_search_items_safely", return_value=iter([])) as mock_safe_items,
    ):
        _, result = stac_harvester.prepare_catalogue_stac("https://example.com")

    list(result)
    mock_safe_items.assert_called_once_with(mock_client, None)
    mock_client.get_all_items.assert_not_called()


@moto.mock_aws()
def test_main_matches_existing_item_search_item_without_changing_identity(
    parameters: dict[str, Any], mocker: MockerFixture
) -> None:
    """The #41 fix must not make unchanged valid items appear newly harvested."""
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=parameters["metadata_bucket_name"])
    s3.put_object(
        Bucket=parameters["metadata_bucket_name"],
        Key=f"harvested-metadata/{parameters['url'].replace('/', '')}",
        Body=json.dumps({"collections/test-collection/items/test-item.json": "hash"}),
    )

    item = mock.MagicMock()
    item.get_self_href.return_value = "https://example.com/items/test-item"

    sys.argv = [
        None,
        parameters["url"],
        "workspace-name",
        "catalog-name",
        "catalogue_target",
        parameters["metadata_bucket_name"],
    ]
    mocker.patch(
        "stac_harvester.__main__.prepare_catalogue_stac",
        return_value=("catalogue_name", [item]),
    )

    mock_pulsar_client = mock.MagicMock()
    mock_producer = mock.MagicMock()
    mock_pulsar_client.create_producer.return_value = mock_producer
    mocker.patch("stac_harvester.__main__.get_pulsar_client", return_value=mock_pulsar_client)

    with (
        mock.patch("stac_harvester.__main__.STACHarvesterMessager") as mock_messager,
        mock.patch("stac_harvester.__main__.parse_url_stac") as mock_parse,
    ):
        mock_parse.return_value = (
            "collections/test-collection/items/test-item",
            {"file content": "unchanged data"},
            "hash",
        )

        mock_messager_instance = mock_messager.return_value
        mock_messager_instance.consume.return_value = None
        stac_harvester.main()

        all_harvested = {}
        all_deleted = []
        for call in mock_messager_instance.consume.call_args_list:
            all_harvested.update(call[0][0].get("harvested_data", {}))
            all_deleted.extend(call[0][0].get("deleted_keys", []))

        assert all_harvested == {}
        assert all_deleted == []


@moto.mock_aws()
def test_main_suppresses_deletions_after_skipping_malformed_search_item(
    parameters: dict[str, Any], mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Malformed item-search features make the run partial, so stale keys cannot be deleted."""
    # Override the circuit breaker so that only partial-run handling can prevent the delete
    monkeypatch.setenv("ALLOW_UNSAFE_DELETES", "true")
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=parameters["metadata_bucket_name"])
    s3.put_object(
        Bucket=parameters["metadata_bucket_name"],
        Key=f"harvested-metadata/{parameters['url'].replace('/', '')}",
        Body=json.dumps({"collections/test-collection/items/broken-item.json": "hash"}),
    )

    mock_client = mock.MagicMock()
    mock_client.id = "test-catalogue"
    mock_client.get_root.return_value = mock.MagicMock()
    mock_client.get_root.return_value.get_self_href.return_value = "https://example.com/root"
    mock_client.get_collections.return_value = []
    mock_client.conforms_to.return_value = True
    mock_search = mock.MagicMock()
    mock_search.url = "https://example.com/search"
    mock_search.items_as_dicts.return_value = [
        {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "broken-item",
            "geometry": None,
            "bbox": None,
            "properties": {},
            "links": [{"rel": "self", "href": "https://example.com/items/broken-item"}],
        },
    ]
    mock_client.search.return_value = mock_search
    mocker.patch("stac_harvester.__main__._create_stac_client", return_value=mock_client)

    sys.argv = [
        None,
        parameters["url"],
        "workspace-name",
        "catalog-name",
        "catalogue_target",
        parameters["metadata_bucket_name"],
    ]

    mock_pulsar_client = mock.MagicMock()
    mock_producer = mock.MagicMock()
    mock_pulsar_client.create_producer.return_value = mock_producer
    mocker.patch("stac_harvester.__main__.get_pulsar_client", return_value=mock_pulsar_client)

    with (
        mock.patch("stac_harvester.__main__.STACHarvesterMessager") as mock_messager,
        mock.patch("stac_harvester.__main__.parse_url_stac") as mock_parse,
    ):
        mock_parse.return_value = ("catalogs/root", {"file content": "root data"}, "root-hash")
        mock_messager_instance = mock_messager.return_value
        mock_messager_instance.consume.return_value = None

        stac_harvester.main()

        all_deleted = []
        for call in mock_messager_instance.consume.call_args_list:
            all_deleted.extend(call[0][0].get("deleted_keys", []))

        assert all_deleted == []


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
        return_value=("catalogue_name", failing_iterator()),
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
def test_main_flushes_on_malformed_iterator_item(
    parameters: dict[str, Any], mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If item construction fails outside the loop body, flush processed data and avoid deletions."""
    # Override the circuit breaker so that only partial-run handling can prevent the delete
    monkeypatch.setenv("ALLOW_UNSAFE_DELETES", "true")
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=parameters["metadata_bucket_name"])
    previous_metadata = {"collections/old/items/old-item.json": "old-hash"}
    metadata_key = f"harvested-metadata/{parameters['url'].replace('/', '')}"
    s3.put_object(
        Bucket=parameters["metadata_bucket_name"],
        Key=metadata_key,
        Body=json.dumps(previous_metadata),
    )

    good_item = mock.MagicMock()
    good_item.get_self_href.return_value = "https://example.com/good-item"

    def malformed_iterator() -> Generator[mock.MagicMock]:
        yield good_item
        raise STACError("Invalid item payload")

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
        return_value=("catalogue_name", malformed_iterator()),
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

        all_harvested = {}
        all_deleted = []
        for call in mock_messager_instance.consume.call_args_list:
            all_harvested.update(call[0][0].get("harvested_data", {}))
            all_deleted.extend(call[0][0].get("deleted_keys", []))

        assert "good-key.json" in all_harvested
        assert all_deleted == []

    stored_metadata = json.loads(
        s3.get_object(Bucket=parameters["metadata_bucket_name"], Key=metadata_key)["Body"].read().decode("utf-8")
    )
    assert stored_metadata == previous_metadata | {"good-key.json": "hash"}


# --- Delete safety: partial runs, collection scoping, circuit breaker, fetch retries ---

ROOT_URL = "https://api.example.com/"
COLLECTION_URL = "https://api.example.com/collections/sentinel1_ard"
ROOT_KEY = "catalogs/ceda-stac-catalogue.json"
COLLECTION_KEY = "catalogs/ceda-stac-catalogue/collections/sentinel1_ard.json"
OWN_ITEM_PREFIX = "catalogs/ceda-stac-catalogue/collections/sentinel1_ard/items/"
FOREIGN_ITEM_KEY = "catalogs/ceda-stac-catalogue/collections/other/items/foreign-item.json"
ITEM_PREFIX = "catalogs/root/collections/c/items/"

Listing = list[tuple[str, str | Exception]]


def _entry(url: str) -> mock.Mock:
    entry = mock.Mock()
    entry.get_self_href.return_value = url
    return entry


def _item_url(name: str) -> str:
    return f"https://example.com/items/{name}"


def _items(names: Iterable[str]) -> Listing:
    """Listing of items that parse successfully to keys under ITEM_PREFIX"""
    return [(_item_url(name), ITEM_PREFIX + name) for name in names]


def _deleted(payloads: list[dict[str, Any]]) -> list[str]:
    return [key for payload in payloads for key in payload["deleted_keys"]]


def _harvested(payloads: list[dict[str, Any]]) -> dict[str, Any]:
    harvested: dict[str, Any] = {}
    for payload in payloads:
        harvested.update(payload["harvested_data"])
    return harvested


def _run_main(
    mocker: MockerFixture,
    parameters: dict[str, Any],
    *,
    previous: dict[str, str] | None = None,
    source_url: str | None = None,
    raises: type[Exception] | None = None,
    consume_results: Iterable[Messager.Failures] = (),
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Run main() against a moto bucket. Returns a copy of every consume() payload, in order, and the
    harvest metadata stored afterwards. consume() returns each of consume_results in turn, and then
    reports no failures"""
    url = source_url or parameters["url"]
    bucket = parameters["metadata_bucket_name"]
    metadata_key = f"harvested-metadata/{url.replace('/', '')}"
    payloads: list[dict[str, Any]] = []

    with moto.mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=bucket)
        if previous is not None:
            s3.put_object(Bucket=bucket, Key=metadata_key, Body=json.dumps(previous))

        mocker.patch.object(sys, "argv", [None, url, "workspace-name", "catalog-name", "catalogue_target", bucket])
        mocker.patch("stac_harvester.__main__.get_pulsar_client", return_value=mock.MagicMock())
        mock_messager = mocker.patch("stac_harvester.__main__.STACHarvesterMessager")
        results = iter(consume_results)

        def consume(message: dict[str, Any]) -> Messager.Failures:
            # main() reuses one message dict, so each payload has to be copied as it is consumed
            payloads.append(copy.deepcopy(message))
            return next(results, Messager.Failures())

        mock_messager.return_value.consume.side_effect = consume

        if raises is None:
            stac_harvester.main()
        else:
            with pytest.raises(raises):
                stac_harvester.main()

        s3_resource: Any = boto3.resource("s3")
        stored = [obj.key for obj in s3_resource.Bucket(bucket).objects.all()]
        state: dict[str, str] = {}
        if metadata_key in stored:
            state = json.loads(s3.get_object(Bucket=bucket, Key=metadata_key)["Body"].read().decode("utf-8"))

    return payloads, state


def _type_of_key(key: str) -> str:
    if stac_harvester.is_item_key(key):
        return "Feature"
    return "Collection" if key.split("/")[-2] == "collections" else "Catalog"


def _patch_listing(mocker: MockerFixture, listing: Listing, *, collection_mode: bool = False) -> mock.Mock:
    """Patch prepare_catalogue_stac to list the given URLs, and parse_url_stac to map each URL to its
    key (with hash "new-hash") or raise its exception. Returns the parse_url_stac mock"""
    outcomes = dict(listing)

    def prepare(source_url: str, run_state: stac_harvester.HarvestRunState) -> tuple[str, list[mock.Mock]]:
        run_state.collection_mode = collection_mode
        return "catalogue_name", [_entry(url) for url, _ in listing]

    def parse(url: str) -> tuple[str, dict[str, Any], str]:
        outcome = outcomes[url]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome, {"id": outcome, "type": _type_of_key(outcome)}, "new-hash"

    mocker.patch("stac_harvester.__main__.prepare_catalogue_stac", side_effect=prepare)
    return mocker.patch("stac_harvester.__main__.parse_url_stac", side_effect=parse)


def _own_item_url(item_id: str) -> str:
    return f"{COLLECTION_URL}/items/{item_id}"


def _patch_collection_source(
    mocker: MockerFixture, item_ids: list[str], *, listed_self: str = COLLECTION_URL, source_url: str = COLLECTION_URL
) -> mock.MagicMock:
    """Make the real prepare_catalogue_stac see COLLECTION_URL as a collection beneath ROOT_URL. Returns
    the mock client for the root catalogue"""
    listed_collection = _entry(listed_self)
    listed_collection.id = "sentinel1_ard"
    listed_collection.links = [mock.Mock(rel="self", target=listed_self)]

    client = mock.MagicMock()
    client.get_root.return_value = _entry(ROOT_URL)
    client.get_collections.return_value = [listed_collection]
    client.get_collection.return_value.get_items.return_value = [_entry(_own_item_url(i)) for i in item_ids]

    def create_client(url: str) -> mock.MagicMock:
        if url == source_url:
            raise ClientTypeError("Not a catalogue")
        return client

    mocker.patch("stac_harvester.__main__._create_stac_client", side_effect=create_client)
    mock_from_file = mocker.patch("stac_harvester.__main__.Collection.from_file")
    mock_from_file.return_value.id = "sentinel1_ard"
    mock_from_file.return_value.links = [mock.Mock(rel="root", target=ROOT_URL)]
    return client


def _ceda_documents(item_ids: list[str]) -> dict[str, bytes]:
    """CEDA-shaped documents: the item parent href is the collection self href, which is the config URL"""
    documents: dict[str, Any] = {
        ROOT_URL: {"type": "Catalog", "id": "ceda-stac-catalogue", "links": [{"rel": "self", "href": ROOT_URL}]},
        COLLECTION_URL: {
            "type": "Collection",
            "id": "sentinel1_ard",
            "links": [
                {"rel": "root", "href": ROOT_URL},
                {"rel": "parent", "href": ROOT_URL},
                {"rel": "self", "href": COLLECTION_URL},
            ],
        },
    }
    for item_id in item_ids:
        documents[_own_item_url(item_id)] = {
            "type": "Feature",
            "id": item_id,
            "links": [
                {"rel": "collection", "href": COLLECTION_URL},
                {"rel": "parent", "href": COLLECTION_URL},
                {"rel": "root", "href": ROOT_URL},
                {"rel": "self", "href": _own_item_url(item_id)},
            ],
        }
    return {url: json.dumps(document).encode("utf-8") for url, document in documents.items()}


def test_main_collection_root_fetch_failure_sends_no_deletes(
    parameters: dict[str, Any], mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Incident regression: a failed fetch of the shared root catalogue must not be treated as a
    removal, and must suppress every other delete in the run"""
    # Override the circuit breaker so that only partial-run handling can prevent the delete
    monkeypatch.setenv("ALLOW_UNSAFE_DELETES", "true")
    item_ids = ["item-1", "item-2"]
    _patch_collection_source(mocker, item_ids)
    documents = _ceda_documents(item_ids)
    root_failures: list[str] = []

    def file_contents(url: str) -> bytes:
        if url == ROOT_URL and not root_failures:
            root_failures.append(url)
            raise stac_harvester.HarvesterFetchError("root is down")
        return documents[url]

    mocker.patch("stac_harvester.__main__.get_file_contents", side_effect=file_contents)
    previous = {
        ROOT_KEY: "old-hash",
        COLLECTION_KEY: "old-hash",
        OWN_ITEM_PREFIX + "item-1.json": "old-hash",
        OWN_ITEM_PREFIX + "item-2.json": "old-hash",
        OWN_ITEM_PREFIX + "item-gone.json": "old-hash",
    }

    payloads, state = _run_main(mocker, parameters, previous=previous, source_url=COLLECTION_URL)

    assert root_failures == [ROOT_URL]
    assert payloads
    assert _deleted(payloads) == []
    assert set(state) == set(previous)
    assert state[ROOT_KEY] == "old-hash"
    assert state[OWN_ITEM_PREFIX + "item-gone.json"] == "old-hash"
    # Entries that were fetched are still recorded with their new hashes
    assert state[COLLECTION_KEY] != "old-hash"
    assert COLLECTION_KEY in _harvested(payloads)


def test_main_collection_total_outage_preserves_state(parameters: dict[str, Any], mocker: MockerFixture) -> None:
    """With the source down nothing is deleted, the state is unchanged and the loop gives up early"""
    item_ids = [f"item-{i}" for i in range(6)]
    _patch_collection_source(mocker, item_ids)
    mock_file_contents = mocker.patch(
        "stac_harvester.__main__.get_file_contents", side_effect=stac_harvester.HarvesterFetchError("down")
    )
    previous = {ROOT_KEY: "old-hash", COLLECTION_KEY: "old-hash"} | {
        f"{OWN_ITEM_PREFIX}{item_id}.json": "old-hash" for item_id in item_ids
    }

    payloads, state = _run_main(mocker, parameters, previous=previous, source_url=COLLECTION_URL)

    assert _deleted(payloads) == []
    assert _harvested(payloads) == {}
    assert state == previous
    # 8 entries are listed, but the harvest aborts rather than attempting them all
    assert mock_file_contents.call_count == stac_harvester.MAX_CONSECUTIVE_FETCH_FAILURES


def test_main_collection_clean_run_keeps_root_key(parameters: dict[str, Any], mocker: MockerFixture) -> None:
    """A clean collection harvest only deletes beneath its own collection and keeps tracking the root"""
    item_ids = ["item-1"]
    _patch_collection_source(mocker, item_ids)
    documents = _ceda_documents(item_ids)
    mocker.patch("stac_harvester.__main__.get_file_contents", side_effect=lambda url: documents[url])
    own_stale_key = OWN_ITEM_PREFIX + "item-gone.json"
    previous = {
        ROOT_KEY: "old-hash",
        COLLECTION_KEY: "old-hash",
        OWN_ITEM_PREFIX + "item-1.json": "old-hash",
        own_stale_key: "old-hash",
        FOREIGN_ITEM_KEY: "old-hash",
    }

    payloads, state = _run_main(mocker, parameters, previous=previous, source_url=COLLECTION_URL)

    assert _deleted(payloads) == [own_stale_key]
    assert set(state) == {ROOT_KEY, COLLECTION_KEY, OWN_ITEM_PREFIX + "item-1.json", FOREIGN_ITEM_KEY}
    assert state[ROOT_KEY] == stac_harvester.get_file_hash(documents[ROOT_URL])
    assert state[FOREIGN_ITEM_KEY] == "old-hash"


def _assert_item_error_marks_run_partial(mocker: MockerFixture, parameters: dict[str, Any], error: Exception) -> None:
    stale_key = ITEM_PREFIX + "stale.json"
    previous = {stale_key: "old-hash", ITEM_PREFIX + "bad.json": "old-hash"}
    _patch_listing(mocker, [*_items(["good"]), (_item_url("bad"), error)])

    payloads, state = _run_main(mocker, parameters, previous=previous)

    assert _deleted(payloads) == []
    assert ITEM_PREFIX + "good.json" in _harvested(payloads)
    assert state == previous | {ITEM_PREFIX + "good.json": "new-hash"}


def test_main_fetch_error_marks_run_partial(
    parameters: dict[str, Any], mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Incident regression: an entry skipped after a fetch failure must suppress all deletes"""
    # Override the circuit breaker so that only partial-run handling can prevent the delete
    monkeypatch.setenv("ALLOW_UNSAFE_DELETES", "true")
    _assert_item_error_marks_run_partial(mocker, parameters, stac_harvester.HarvesterFetchError("fetch failed"))


def test_main_unexpected_item_error_marks_run_partial(
    parameters: dict[str, Any], mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An entry skipped for any other reason must suppress all deletes too"""
    # Override the circuit breaker so that only partial-run handling can prevent the delete
    monkeypatch.setenv("ALLOW_UNSAFE_DELETES", "true")
    _assert_item_error_marks_run_partial(mocker, parameters, ValueError("not JSON"))


def test_main_partial_run_merges_state(
    parameters: dict[str, Any], mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After a partial run new hashes are recorded and every unseen key keeps its old hash"""
    # Override the circuit breaker so that only partial-run handling can prevent the delete
    monkeypatch.setenv("ALLOW_UNSAFE_DELETES", "true")
    key_a, key_b, key_c = (ITEM_PREFIX + f"{name}.json" for name in "abc")
    previous = {key_a: "h1", key_b: "h2", key_c: "h3"}
    _patch_listing(mocker, [*_items(["a"]), (_item_url("b"), stac_harvester.HarvesterFetchError("fetch failed"))])

    payloads, state = _run_main(mocker, parameters, previous=previous)

    assert _deleted(payloads) == []
    assert list(_harvested(payloads)) == [key_a]
    assert state == {key_a: "new-hash", key_b: "h2", key_c: "h3"}


def test_main_aborts_after_consecutive_fetch_failures(
    parameters: dict[str, Any], mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A source that is down must not be hammered for every listed entry"""
    # Override the circuit breaker so that only partial-run handling can prevent the delete
    monkeypatch.setenv("ALLOW_UNSAFE_DELETES", "true")
    error = stac_harvester.HarvesterFetchError("fetch failed")
    mock_parse = _patch_listing(mocker, [(_item_url(f"item-{i}"), error) for i in range(10)])

    payloads, state = _run_main(mocker, parameters, previous={ITEM_PREFIX + "item-0.json": "old-hash"})

    assert mock_parse.call_count == stac_harvester.MAX_CONSECUTIVE_FETCH_FAILURES == 5
    # The final flush still happens, with nothing to delete
    assert payloads == [
        {"source": "catalog-name", "target": "catalogue_target", "harvested_data": {}, "deleted_keys": []}
    ]
    assert state == {ITEM_PREFIX + "item-0.json": "old-hash"}


def test_main_consecutive_failure_counter_resets_on_success(parameters: dict[str, Any], mocker: MockerFixture) -> None:
    """Scattered failures below the consecutive limit do not abort the harvest"""
    error = stac_harvester.HarvesterFetchError("fetch failed")
    failing_before: Listing = [(_item_url(f"bad-{i}"), error) for i in range(4)]
    failing_after: Listing = [(_item_url(f"bad-{i}"), error) for i in range(4, 8)]
    mock_parse = _patch_listing(mocker, [*failing_before, *_items(["good"]), *failing_after])

    payloads, _ = _run_main(mocker, parameters)

    assert mock_parse.call_count == 9
    assert _deleted(payloads) == []


def test_main_aborts_after_total_fetch_failures(
    parameters: dict[str, Any], mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failures that never run consecutively still abort the harvest once there are too many"""
    # Override the circuit breaker so that only partial-run handling can prevent the delete
    monkeypatch.setenv("ALLOW_UNSAFE_DELETES", "true")
    error = stac_harvester.HarvesterFetchError("fetch failed")
    listing: Listing = []
    for i in range(stac_harvester.MAX_TOTAL_FETCH_FAILURES + 10):
        listing.append((_item_url(f"bad-{i}"), error))
        listing.extend(_items([f"good-{i}"]))
    mock_parse = _patch_listing(mocker, listing)

    payloads, state = _run_main(mocker, parameters, previous={ITEM_PREFIX + "stale.json": "old-hash"})

    # Aborts on the 50th failure, having harvested the 49 good entries in between
    assert mock_parse.call_count == 2 * stac_harvester.MAX_TOTAL_FETCH_FAILURES - 1
    assert _deleted(payloads) == []
    assert len(state) == stac_harvester.MAX_TOTAL_FETCH_FAILURES
    assert state[ITEM_PREFIX + "stale.json"] == "old-hash"


def test_main_flushes_on_api_error_during_pagination(
    parameters: dict[str, Any], mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A pystac-client APIError while paginating flushes and merges state instead of crashing"""
    # Override the circuit breaker so that only partial-run handling can prevent the delete
    monkeypatch.setenv("ALLOW_UNSAFE_DELETES", "true")
    previous = {ITEM_PREFIX + "stale.json": "old-hash"}

    def failing_iterator() -> Generator[mock.Mock]:
        yield _entry(_item_url("good"))
        raise APIError("502 Bad Gateway")

    mocker.patch("stac_harvester.__main__.prepare_catalogue_stac", return_value=("catalogue", failing_iterator()))
    mocker.patch(
        "stac_harvester.__main__.parse_url_stac", return_value=(ITEM_PREFIX + "good", {"id": "good"}, "new-hash")
    )

    payloads, state = _run_main(mocker, parameters, previous=previous)

    assert ITEM_PREFIX + "good.json" in _harvested(payloads)
    assert _deleted(payloads) == []
    assert state == previous | {ITEM_PREFIX + "good.json": "new-hash"}


def test_main_collection_mode_never_deletes_out_of_scope_keys(
    parameters: dict[str, Any], mocker: MockerFixture
) -> None:
    """A collection harvester can only delete keys beneath its own collection"""
    own_stale_key = OWN_ITEM_PREFIX + "item-gone.json"
    previous = {
        ROOT_KEY: "root-hash",
        FOREIGN_ITEM_KEY: "foreign-hash",
        COLLECTION_KEY: "old-hash",
        OWN_ITEM_PREFIX + "item-1.json": "old-hash",
        own_stale_key: "old-hash",
    }
    listing: Listing = [
        (parameters["url"], COLLECTION_KEY.removesuffix(".json")),
        (_item_url("item-1"), OWN_ITEM_PREFIX + "item-1"),
    ]
    _patch_listing(mocker, listing, collection_mode=True)

    payloads, state = _run_main(mocker, parameters, previous=previous)

    assert _deleted(payloads) == [own_stale_key]
    assert state == {
        ROOT_KEY: "root-hash",
        FOREIGN_ITEM_KEY: "foreign-hash",
        COLLECTION_KEY: "new-hash",
        OWN_ITEM_PREFIX + "item-1.json": "new-hash",
    }


def test_main_collection_mode_without_owned_prefix_deletes_nothing(
    parameters: dict[str, Any], mocker: MockerFixture
) -> None:
    """If the configured collection itself was never harvested, nothing is known to be owned"""
    previous = {ROOT_KEY: "root-hash", OWN_ITEM_PREFIX + "item-gone.json": "old-hash"}
    _patch_listing(mocker, [(_item_url("item-1"), OWN_ITEM_PREFIX + "item-1")], collection_mode=True)

    payloads, state = _run_main(mocker, parameters, previous=previous)

    assert _deleted(payloads) == []
    assert state == previous | {OWN_ITEM_PREFIX + "item-1.json": "new-hash"}


def test_main_full_catalogue_mode_deletes_unscoped(parameters: dict[str, Any], mocker: MockerFixture) -> None:
    """A clean full-catalogue run sends the same messages and stores the same state as before"""
    kept_key = ITEM_PREFIX + "kept.json"
    stale_key = ITEM_PREFIX + "stale.json"
    _patch_listing(mocker, _items(["kept"]))

    payloads, state = _run_main(mocker, parameters, previous={kept_key: "new-hash", stale_key: "old-hash"})

    message = {"source": "catalog-name", "target": "catalogue_target", "harvested_data": {}}
    assert payloads == [message | {"deleted_keys": []}, message | {"deleted_keys": [stale_key]}]
    assert state == {kept_key: "new-hash"}


def test_prepare_catalogue_raises_when_collection_not_found(mocker: MockerFixture) -> None:
    """A collection that is missing upstream must fail the run rather than harvest only the root"""
    _patch_collection_source(mocker, ["item-1"], listed_self="https://api.example.com/collections/other")

    with pytest.raises(stac_harvester.CollectionNotFoundError, match="not found under root catalogue"):
        stac_harvester.prepare_catalogue_stac(COLLECTION_URL, stac_harvester.HarvestRunState())


def test_prepare_catalogue_sets_collection_mode(mocker: MockerFixture) -> None:
    """Harvesting a collection URL lists root, collection and items, and flags collection mode"""
    _patch_collection_source(mocker, ["item-1", "item-2"])
    run_state = stac_harvester.HarvestRunState()

    name, pages = stac_harvester.prepare_catalogue_stac(COLLECTION_URL, run_state)

    assert name == "sentinel1_ard"
    assert run_state.collection_mode is True
    assert [page.get_self_href() for page in pages] == [
        ROOT_URL,
        COLLECTION_URL,
        _own_item_url("item-1"),
        _own_item_url("item-2"),
    ]


def test_prepare_catalogue_full_mode_leaves_collection_mode_unset() -> None:
    """Harvesting a whole catalogue is not collection mode"""
    mock_client = mock.MagicMock()
    mock_client.get_collections.return_value = []
    mock_client.conforms_to.return_value = False
    mock_client.get_all_items.return_value = []
    run_state = stac_harvester.HarvestRunState()

    with mock.patch("stac_harvester.__main__._create_stac_client", return_value=mock_client):
        stac_harvester.prepare_catalogue_stac("https://example.com", run_state)

    assert run_state.collection_mode is False


def test_main_collection_not_found_preserves_state(parameters: dict[str, Any], mocker: MockerFixture) -> None:
    """A missing collection raises before anything is sent or written"""
    _patch_collection_source(mocker, ["item-1"], listed_self="https://api.example.com/collections/other")
    mock_file_contents = mocker.patch("stac_harvester.__main__.get_file_contents")
    previous = {ROOT_KEY: "old-hash", COLLECTION_KEY: "old-hash", OWN_ITEM_PREFIX + "item-1.json": "old-hash"}

    payloads, state = _run_main(
        mocker,
        parameters,
        previous=previous,
        source_url=COLLECTION_URL,
        raises=stac_harvester.CollectionNotFoundError,
    )

    assert payloads == []
    assert state == previous
    mock_file_contents.assert_not_called()


def _item_state(count: int) -> dict[str, str]:
    return {f"{ITEM_PREFIX}item-{i}.json": "new-hash" for i in range(count)}


def _run_with_stale_items(
    mocker: MockerFixture, parameters: dict[str, Any], previous_count: int, stale_count: int
) -> tuple[list[dict[str, Any]], dict[str, str], dict[str, str], list[str]]:
    """Clean full-catalogue run in which the last stale_count of previous_count items are no longer
    listed. Returns (payloads, stored state, previous state, stale keys)"""
    previous = _item_state(previous_count)
    listed = previous_count - stale_count
    stale_keys = list(previous)[listed:]
    _patch_listing(mocker, _items(f"item-{i}" for i in range(listed)))

    payloads, state = _run_main(mocker, parameters, previous=previous)
    return payloads, state, previous, stale_keys


def test_main_circuit_breaker_blocks_container_delete(parameters: dict[str, Any], mocker: MockerFixture) -> None:
    """Catalogue and collection deletes cascade downstream, so they are never sent automatically"""
    stale_key = "catalogs/root/collections/gone.json"
    previous = {stale_key: "old-hash", ITEM_PREFIX + "kept.json": "new-hash"}
    _patch_listing(mocker, _items(["kept"]))

    payloads, state = _run_main(mocker, parameters, previous=previous)

    assert _deleted(payloads) == []
    assert state == previous


def test_main_circuit_breaker_blocks_mass_delete(parameters: dict[str, Any], mocker: MockerFixture) -> None:
    """Losing most of a catalogue in one run looks like a truncated listing, not a real removal"""
    payloads, state, previous, stale_keys = _run_with_stale_items(mocker, parameters, 50, 30)

    assert len(stale_keys) == 30
    assert _deleted(payloads) == []
    # Every stale key is retained, so a later override run can still delete them
    assert state == previous


@pytest.mark.parametrize(("previous_count", "stale_count"), [(50, 5), (50, 10), (100, 11), (100, 20)])
def test_main_circuit_breaker_allows_small_delete(
    parameters: dict[str, Any], mocker: MockerFixture, previous_count: int, stale_count: int
) -> None:
    """Deletes at or below the count floor, or within the allowed fraction, are sent"""
    payloads, state, _, stale_keys = _run_with_stale_items(mocker, parameters, previous_count, stale_count)

    assert _deleted(payloads) == stale_keys
    assert state == _item_state(previous_count - stale_count)


def test_main_circuit_breaker_override(
    parameters: dict[str, Any], mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ALLOW_UNSAFE_DELETES lets an operator push blocked deletes through"""
    monkeypatch.setenv("ALLOW_UNSAFE_DELETES", "true")

    payloads, state, _, stale_keys = _run_with_stale_items(mocker, parameters, 50, 30)

    assert _deleted(payloads) == stale_keys
    assert state == _item_state(20)


def test_main_circuit_breaker_fraction_env(
    parameters: dict[str, Any], mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """MAX_DELETE_FRACTION changes the mass-delete threshold"""
    monkeypatch.setenv("MAX_DELETE_FRACTION", "0.9")

    payloads, state, _, stale_keys = _run_with_stale_items(mocker, parameters, 50, 30)

    assert _deleted(payloads) == stale_keys
    assert state == _item_state(20)


def test_main_circuit_breaker_blocks_when_no_items_listed(parameters: dict[str, Any], mocker: MockerFixture) -> None:
    """An empty 200 listing looks like a clean run; it must not delete a small collection's items"""
    previous = {"catalogs/root.json": "new-hash", ITEM_PREFIX + "only-item.json": "old-hash"}
    _patch_listing(mocker, [("https://example.com/", "catalogs/root")])

    payloads, state = _run_main(mocker, parameters, previous=previous)

    assert _deleted(payloads) == []
    assert state == previous


def test_main_circuit_breaker_allows_delete_when_an_item_is_still_listed(
    parameters: dict[str, Any], mocker: MockerFixture
) -> None:
    """The no-items rule does not get in the way of ordinary item removals"""
    payloads, state, _, stale_keys = _run_with_stale_items(mocker, parameters, 2, 1)

    assert _deleted(payloads) == stale_keys
    assert state == _item_state(1)


def test_main_circuit_breaker_no_items_override(
    parameters: dict[str, Any], mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ALLOW_UNSAFE_DELETES also bypasses the no-items rule"""
    monkeypatch.setenv("ALLOW_UNSAFE_DELETES", "true")
    stale_key = ITEM_PREFIX + "only-item.json"
    _patch_listing(mocker, [("https://example.com/", "catalogs/root")])

    payloads, state = _run_main(mocker, parameters, previous={"catalogs/root.json": "new-hash", stale_key: "old-hash"})

    assert _deleted(payloads) == [stale_key]
    assert state == {"catalogs/root.json": "new-hash"}


def test_main_git_harvest_not_subject_to_breaker(parameters: dict[str, Any], mocker: MockerFixture) -> None:
    """Git harvests are all-or-nothing at fetch time and user-driven, so collections can be removed"""
    source_url = "https://github.com/owner/repo/blob/main/catalogue.json"
    stale_key = "repo/collections/x.json"
    mocker.patch("stac_harvester.__main__.prepare_catalogue_git", return_value=("catalogue", [], "repo"))

    payloads, state = _run_main(
        mocker, parameters, previous={"repo.json": "old-hash", stale_key: "old-hash"}, source_url=source_url
    )

    assert _deleted(payloads) == [stale_key]
    assert list(state) == ["repo.json"]


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ("catalogs/ceda-stac-catalogue.json", False),
        ("catalogs/ceda-stac-catalogue/collections/sentinel1_ard.json", False),
        ("catalogs/ceda-stac-catalogue/collections/sentinel1_ard/items/item-1.json", True),
        ("repo.json", False),
        ("catalogs/root/catalogs/nested.json", False),
        ("catalogs/root/collections/items.json", False),
        ("items/item-1.json", True),
    ],
)
def test_is_item_key(key: str, expected: bool) -> None:
    assert stac_harvester.is_item_key(key) is expected


OWNED_PREFIX = COLLECTION_KEY.removesuffix(".json")
SIBLING_ITEM_KEY = "catalogs/ceda-stac-catalogue/collections/sentinel1_ard_v2/items/item-1.json"


@pytest.mark.parametrize(
    ("collection_mode", "owned_prefix", "expected_deletable", "expected_out_of_scope"),
    [
        # Full-catalogue mode owns everything
        (False, None, [ROOT_KEY, COLLECTION_KEY, OWN_ITEM_PREFIX + "a.json", FOREIGN_ITEM_KEY, SIBLING_ITEM_KEY], []),
        # Collection mode owns its collection key and anything beneath it
        (
            True,
            OWNED_PREFIX,
            [COLLECTION_KEY, OWN_ITEM_PREFIX + "a.json"],
            [ROOT_KEY, FOREIGN_ITEM_KEY, SIBLING_ITEM_KEY],
        ),
        # Collection mode with no known collection key owns nothing
        (True, None, [], [ROOT_KEY, COLLECTION_KEY, OWN_ITEM_PREFIX + "a.json", FOREIGN_ITEM_KEY, SIBLING_ITEM_KEY]),
    ],
)
def test_split_stale_keys(
    collection_mode: bool, owned_prefix: str | None, expected_deletable: list[str], expected_out_of_scope: list[str]
) -> None:
    stale_keys = [ROOT_KEY, COLLECTION_KEY, OWN_ITEM_PREFIX + "a.json", FOREIGN_ITEM_KEY, SIBLING_ITEM_KEY]
    run_state = stac_harvester.HarvestRunState(collection_mode=collection_mode, owned_prefix=owned_prefix)

    assert stac_harvester.split_stale_keys(stale_keys, run_state) == (expected_deletable, expected_out_of_scope)


def _keys(count: int) -> list[str]:
    return list(_item_state(count))


@pytest.mark.parametrize(
    ("deleted_keys", "previous_keys", "latest_keys", "env", "expected"),
    [
        # Nothing to delete
        ([], _keys(5), _keys(5), {}, None),
        ([], _keys(5), [], {}, None),
        # Containers are never deleted, however few
        ([ROOT_KEY], [ROOT_KEY, *_keys(100)], _keys(100), {}, "would delete 1 catalogue/collection keys"),
        ([COLLECTION_KEY, *_keys(1)], [COLLECTION_KEY, *_keys(100)], _keys(99), {}, "catalogue/collection keys"),
        ([ROOT_KEY], [ROOT_KEY], [], {"ALLOW_UNSAFE_DELETES": "true"}, None),
        # A listing with no items at all, regardless of the count floor
        (_keys(1), [ROOT_KEY, *_keys(1)], [ROOT_KEY], {}, "listing returned no items but 1 were previously harvested"),
        (_keys(3), _keys(3), [], {"MAX_DELETE_FRACTION": "1"}, "listing returned no items but 3 were"),
        (_keys(1), _keys(2), _keys(2)[1:], {}, None),
        (_keys(1), _keys(1), [], {"ALLOW_UNSAFE_DELETES": "1"}, None),
        # Count floor and fraction
        (_keys(10), _keys(11), _keys(11)[10:], {}, None),
        (_keys(11), _keys(12), _keys(12)[11:], {}, "would delete 11 of 12 previously harvested keys"),
        (_keys(11), _keys(100), _keys(100)[11:], {}, None),
        (_keys(20), _keys(100), _keys(100)[20:], {}, None),
        (_keys(21), _keys(100), _keys(100)[21:], {}, "would delete 21 of 100 previously harvested keys"),
        (_keys(21), _keys(100), _keys(100)[21:], {"MAX_DELETE_FRACTION": "0.5"}, None),
        (_keys(21), _keys(100), _keys(100)[21:], {"ALLOW_UNSAFE_DELETES": "YES"}, None),
        (_keys(21), _keys(100), _keys(100)[21:], {"ALLOW_UNSAFE_DELETES": "false"}, "would delete 21 of 100"),
    ],
)
def test_get_delete_block_reason(
    monkeypatch: pytest.MonkeyPatch,
    deleted_keys: list[str],
    previous_keys: list[str],
    latest_keys: list[str],
    env: dict[str, str],
    expected: str | None,
) -> None:
    for name, value in env.items():
        monkeypatch.setenv(name, value)

    reason = stac_harvester.get_delete_block_reason(
        deleted_keys, previous_keys, latest_keys, stac_harvester.get_max_delete_fraction()
    )

    if expected is None:
        assert reason is None
    else:
        assert reason is not None
        assert expected in reason


FETCH_URL = "https://example.com/item.json"


def _response(read: Callable[[], bytes]) -> mock.MagicMock:
    response = mock.MagicMock()
    response.__enter__.return_value.read.side_effect = read
    response.__exit__.return_value = False
    return response


def _http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(FETCH_URL, code, "error", Message(), None)


def _raise_timeout() -> bytes:
    raise TimeoutError("The read operation timed out")


def test_get_file_contents_retries_with_backoff(mocker: MockerFixture) -> None:
    mock_sleep = mocker.patch("stac_harvester.__main__.time.sleep")
    error = urllib.error.URLError("connection refused")
    mock_urlopen = mocker.patch(
        "stac_harvester.__main__.urlopen", side_effect=[error, error, _response(lambda: b"body")]
    )

    assert stac_harvester.get_file_contents(FETCH_URL) == b"body"
    assert mock_urlopen.call_count == 3
    mock_urlopen.assert_called_with(FETCH_URL, timeout=stac_harvester.FETCH_TIMEOUT_SECONDS)
    assert [call.args[0] for call in mock_sleep.call_args_list] == [2, 4]


def test_get_file_contents_retries_read_timeout(mocker: MockerFixture) -> None:
    """A timeout while reading the body is retried, not just one while connecting"""
    mock_sleep = mocker.patch("stac_harvester.__main__.time.sleep")
    mock_urlopen = mocker.patch(
        "stac_harvester.__main__.urlopen", side_effect=[_response(_raise_timeout), _response(lambda: b"body")]
    )

    assert stac_harvester.get_file_contents(FETCH_URL) == b"body"
    assert mock_urlopen.call_count == 2
    assert [call.args[0] for call in mock_sleep.call_args_list] == [2]


def test_get_file_contents_retries_incomplete_read(mocker: MockerFixture) -> None:
    mocker.patch("stac_harvester.__main__.time.sleep")

    def incomplete_read() -> bytes:
        raise http.client.IncompleteRead(b"partial")

    mock_urlopen = mocker.patch(
        "stac_harvester.__main__.urlopen", side_effect=[_response(incomplete_read), _response(lambda: b"body")]
    )

    assert stac_harvester.get_file_contents(FETCH_URL) == b"body"
    assert mock_urlopen.call_count == 2


def test_get_file_contents_raises_after_max_attempts(mocker: MockerFixture) -> None:
    mock_sleep = mocker.patch("stac_harvester.__main__.time.sleep")
    mock_urlopen = mocker.patch(
        "stac_harvester.__main__.urlopen", side_effect=urllib.error.URLError("connection refused")
    )

    with pytest.raises(stac_harvester.HarvesterFetchError, match="after 4 attempts"):
        stac_harvester.get_file_contents(FETCH_URL)

    assert mock_urlopen.call_count == stac_harvester.FETCH_ATTEMPTS == 4
    assert [call.args[0] for call in mock_sleep.call_args_list] == [2, 4, 8]


@pytest.mark.parametrize("code", [400, 404])
def test_get_file_contents_does_not_retry_404(mocker: MockerFixture, code: int) -> None:
    """Client errors will not recover, so they fail straight away"""
    mock_sleep = mocker.patch("stac_harvester.__main__.time.sleep")
    mock_urlopen = mocker.patch("stac_harvester.__main__.urlopen", side_effect=_http_error(code))

    with pytest.raises(stac_harvester.HarvesterFetchError, match=f"HTTP {code}"):
        stac_harvester.get_file_contents(FETCH_URL)

    assert mock_urlopen.call_count == 1
    mock_sleep.assert_not_called()


@pytest.mark.parametrize("code", [408, 429, 503])
def test_get_file_contents_retries_transient_http_errors(mocker: MockerFixture, code: int) -> None:
    mock_sleep = mocker.patch("stac_harvester.__main__.time.sleep")
    mock_urlopen = mocker.patch("stac_harvester.__main__.urlopen", side_effect=_http_error(code))

    with pytest.raises(stac_harvester.HarvesterFetchError, match="after 4 attempts"):
        stac_harvester.get_file_contents(FETCH_URL)

    assert mock_urlopen.call_count == stac_harvester.FETCH_ATTEMPTS
    assert mock_sleep.call_count == stac_harvester.FETCH_ATTEMPTS - 1


def test_get_file_contents_does_not_retry_invalid_url(mocker: MockerFixture) -> None:
    """InvalidURL is an HTTPException, not a network failure, so it is not swallowed or retried"""
    mock_sleep = mocker.patch("stac_harvester.__main__.time.sleep")
    mock_urlopen = mocker.patch(
        "stac_harvester.__main__.urlopen", side_effect=http.client.InvalidURL("URL can't contain control characters")
    )

    with pytest.raises(http.client.InvalidURL):
        stac_harvester.get_file_contents(FETCH_URL)

    assert mock_urlopen.call_count == 1
    mock_sleep.assert_not_called()


# --- Review fixes: listing validation, entry validation, publish failures, configuration ---

API_URL = "https://stac.example.com"
SEARCH_URL = f"{API_URL}/search"
COLLECTIONS_URL = f"{API_URL}/collections"
API_ROOT: dict[str, Any] = {
    "type": "Catalog",
    "id": "root",
    "stac_version": "1.0.0",
    "description": "Root catalogue",
    "conformsTo": [
        "https://api.stacspec.org/v1.0.0/core",
        "https://api.stacspec.org/v1.0.0/collections",
        "https://api.stacspec.org/v1.0.0/item-search",
    ],
    "links": [
        {"rel": "self", "href": API_URL},
        {"rel": "root", "href": API_URL},
        {"rel": "data", "href": COLLECTIONS_URL},
        {"rel": "search", "type": "application/geo+json", "href": SEARCH_URL},
    ],
}


def _api_item(item_id: str) -> dict[str, Any]:
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": item_id,
        "geometry": None,
        "bbox": None,
        "properties": {"datetime": "2020-01-01T00:00:00Z"},
        "links": [{"rel": "self", "href": f"{API_URL}/collections/c/items/{item_id}"}],
        "assets": {},
    }


def _spec_entry(spec: type, entry_id: str, url: str) -> mock.Mock:
    """A listed entry that passes for a real pystac object of the given class"""
    entry = mock.Mock(spec=spec)
    entry.id = entry_id
    entry.get_self_href.return_value = url
    return entry


def test_main_malformed_later_page_sends_no_deletes(
    parameters: dict[str, Any],
    mocker: MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
    requests_mock: RequestsMocker,
) -> None:
    """A later listing page answering 200 with no features is a truncated listing, not the end of it"""
    monkeypatch.setenv("ALLOW_UNSAFE_DELETES", "true")
    requests_mock.get(API_URL, json=API_ROOT)
    requests_mock.get(COLLECTIONS_URL, json={"collections": [], "links": []})
    next_link = {"rel": "next", "href": f"{SEARCH_URL}?page=2"}
    requests_mock.post(
        SEARCH_URL, json={"type": "FeatureCollection", "features": [_api_item("kept")], "links": [next_link]}
    )
    requests_mock.get(f"{SEARCH_URL}?page=2", json={})
    documents = {
        API_URL: json.dumps(API_ROOT).encode("utf-8"),
        f"{API_URL}/collections/c/items/kept": json.dumps(_api_item("kept")).encode("utf-8"),
    }
    mocker.patch("stac_harvester.__main__.get_file_contents", side_effect=lambda url: documents[url])
    previous = {"catalogs/root.json": "old-hash", "items/kept.json": "old-hash", "items/omitted.json": "old-hash"}

    payloads, state = _run_main(mocker, parameters, previous=previous, source_url=API_URL)

    assert "items/kept.json" in _harvested(payloads)
    assert _deleted(payloads) == []
    assert set(state) == set(previous)
    assert state["items/omitted.json"] == "old-hash"


def test_main_complete_pagination_still_deletes(
    parameters: dict[str, Any], mocker: MockerFixture, requests_mock: RequestsMocker
) -> None:
    """Listing validation does not get in the way of a listing that really did end"""
    requests_mock.get(API_URL, json=API_ROOT)
    requests_mock.get(COLLECTIONS_URL, json={"collections": [], "links": []})
    next_link = {"rel": "next", "href": f"{SEARCH_URL}?page=2"}
    requests_mock.post(
        SEARCH_URL, json={"type": "FeatureCollection", "features": [_api_item("kept")], "links": [next_link]}
    )
    requests_mock.get(f"{SEARCH_URL}?page=2", json={"type": "FeatureCollection", "features": [], "links": []})
    documents = {
        API_URL: json.dumps(API_ROOT).encode("utf-8"),
        f"{API_URL}/collections/c/items/kept": json.dumps(_api_item("kept")).encode("utf-8"),
    }
    mocker.patch("stac_harvester.__main__.get_file_contents", side_effect=lambda url: documents[url])
    previous = {"catalogs/root.json": "old-hash", "items/kept.json": "old-hash", "items/omitted.json": "old-hash"}

    payloads, state = _run_main(mocker, parameters, previous=previous, source_url=API_URL)

    assert _deleted(payloads) == ["items/omitted.json"]
    assert set(state) == {"catalogs/root.json", "items/kept.json"}


@pytest.mark.parametrize("page", [{}, {"features": None}, {"collections": "oops"}, [], {"detail": "error"}])
def test_listing_io_rejects_malformed_first_page(requests_mock: RequestsMocker, page: Any) -> None:  # noqa: ANN401
    requests_mock.get(COLLECTIONS_URL, json=page)

    with pytest.raises(APIError, match="Malformed listing page"):
        list(stac_harvester._create_stac_io().get_pages(COLLECTIONS_URL))


@pytest.mark.parametrize("page", [{"features": []}, {"collections": [], "links": []}])
def test_listing_io_accepts_empty_listing(requests_mock: RequestsMocker, page: dict[str, Any]) -> None:
    """A legitimately empty listing is valid; the no-items breaker rule covers that case"""
    requests_mock.get(COLLECTIONS_URL, json=page)

    assert list(stac_harvester._create_stac_io().get_pages(COLLECTIONS_URL)) == []


def test_listing_io_only_validates_listing_pages(requests_mock: RequestsMocker) -> None:
    """Other documents read through the same IO, even part-way through a listing, are not pages"""
    collection_url = f"{COLLECTIONS_URL}/c"
    requests_mock.get(COLLECTIONS_URL, json={"collections": [{"id": "c"}], "links": []})
    requests_mock.get(collection_url, json={"type": "Collection", "id": "c"})
    stac_io = stac_harvester._create_stac_io()

    assert stac_io.read_json(collection_url) == {"type": "Collection", "id": "c"}
    for page in stac_io.get_pages(COLLECTIONS_URL):
        assert page["collections"] == [{"id": "c"}]
        assert stac_io.read_json(collection_url) == {"type": "Collection", "id": "c"}


def test_create_stac_client_keeps_timeout(requests_mock: RequestsMocker) -> None:
    """PystacClient.open resets the IO's timeout unless it is given one as well"""
    requests_mock.get(API_URL, json=API_ROOT)

    client = stac_harvester._create_stac_client(API_URL)

    assert isinstance(client._stac_io, stac_harvester.ListingValidatingStacApiIO)
    assert client._stac_io.timeout == stac_harvester.FETCH_TIMEOUT_SECONDS


def test_prepare_catalogue_reads_collection_with_retrying_io(mocker: MockerFixture) -> None:
    _patch_collection_source(mocker, ["item-1"])

    stac_harvester.prepare_catalogue_stac(COLLECTION_URL, stac_harvester.HarvestRunState())

    from_file: Any = stac_harvester.Collection.from_file
    from_file.assert_called_once_with(COLLECTION_URL, stac_io=mock.ANY)
    stac_io = from_file.call_args[1]["stac_io"]
    assert isinstance(stac_io, stac_harvester.ListingValidatingStacApiIO)
    assert stac_io.timeout == stac_harvester.FETCH_TIMEOUT_SECONDS


def test_prepare_catalogue_source_that_is_not_a_catalogue_or_collection(requests_mock: RequestsMocker) -> None:
    """Mode detection is unchanged: a source that is neither still fails with pystac's type error"""
    requests_mock.get(API_URL, json=_api_item("an-item"))

    with pytest.raises(STACTypeError):
        stac_harvester.prepare_catalogue_stac(API_URL, stac_harvester.HarvestRunState())


@pytest.mark.parametrize(
    "document",
    [
        {"type": "Bogus", "id": "a", "links": []},
        {"type": "Feature", "id": "a", "links": [{"rel": "parent", "href": "https://example.com/parent"}]},
    ],
)
def test_parse_url_stac_rejects_unknown_type(mocker: MockerFixture, document: dict[str, Any]) -> None:
    """An entry or ancestor of unknown type must not produce a key"""
    documents = {
        "https://example.com/a": json.dumps(document).encode("utf-8"),
        "https://example.com/parent": b'{"type": "Bogus", "id": "parent", "links": []}',
    }
    mocker.patch("stac_harvester.__main__.get_file_contents", side_effect=lambda url: documents[url])

    with pytest.raises(ValueError, match="Unrecognised STAC type 'Bogus'"):
        stac_harvester.parse_url_stac("https://example.com/a")


@pytest.mark.parametrize(
    ("spec", "data", "valid"),
    [
        (Item, {"type": "Feature", "id": "a"}, True),
        (Collection, {"type": "Collection", "id": "a"}, True),
        (Catalog, {"type": "Catalog", "id": "a"}, True),
        (Item, {"type": "Feature", "id": "b"}, False),
        (Item, {"type": "Feature"}, False),
        (Item, {"type": "Collection", "id": "a"}, False),
        (Collection, {"type": "Catalog", "id": "a"}, False),
        (Catalog, {"type": "Collection", "id": "a"}, False),
        (Catalog, {"type": "Feature", "id": "a"}, False),
    ],
)
def test_validate_fetched_entry(spec: type, data: dict[str, Any], valid: bool) -> None:
    entry = _spec_entry(spec, "a", "https://example.com/a")

    if valid:
        stac_harvester.validate_fetched_entry(entry, data)
    else:
        with pytest.raises(ValueError, match="does not match listed"):
            stac_harvester.validate_fetched_entry(entry, data)


@pytest.mark.parametrize(
    "fetched",
    [
        {"type": "Bogus", "id": "a", "links": []},
        {"type": "Feature", "id": "someone-else", "links": []},
        {"type": "Collection", "id": "a", "links": []},
        {"type": "Bogus", "id": "someone-else", "links": []},
    ],
    ids=["bogus-type", "id-mismatch", "type-mismatch", "both"],
)
def test_main_fetched_entry_not_matching_listing_marks_run_partial(
    parameters: dict[str, Any], mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch, fetched: dict[str, Any]
) -> None:
    """A listed item whose URL serves something else must not be seen as removed"""
    monkeypatch.setenv("ALLOW_UNSAFE_DELETES", "true")
    entries = [_spec_entry(Item, name, _item_url(name)) for name in ("good", "a")]
    mocker.patch("stac_harvester.__main__.prepare_catalogue_stac", return_value=("catalogue", entries))
    documents = {
        _item_url("good"): b'{"type": "Feature", "id": "good", "links": []}',
        _item_url("a"): json.dumps(fetched).encode("utf-8"),
    }
    mocker.patch("stac_harvester.__main__.get_file_contents", side_effect=lambda url: documents[url])
    previous = {"items/good.json": "old-hash", "items/a.json": "old-hash"}

    payloads, state = _run_main(mocker, parameters, previous=previous)

    assert list(_harvested(payloads)) == ["items/good.json"]
    assert _deleted(payloads) == []
    assert set(state) == set(previous)
    assert state["items/a.json"] == "old-hash"


def test_main_collection_url_serving_a_catalogue_does_not_widen_ownership(
    parameters: dict[str, Any], mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only a Collection fetched from the configured URL can define what this harvester owns, even
    when the circuit breaker is overridden"""
    monkeypatch.setenv("ALLOW_UNSAFE_DELETES", "true")
    _patch_collection_source(mocker, ["item-1"])
    documents = _ceda_documents(["item-1"])
    documents[COLLECTION_URL] = documents[ROOT_URL]
    mocker.patch("stac_harvester.__main__.get_file_contents", side_effect=lambda url: documents[url])
    previous = {ROOT_KEY: "old-hash", FOREIGN_ITEM_KEY: "old-hash"}

    payloads, state = _run_main(mocker, parameters, previous=previous, source_url=COLLECTION_URL)

    assert _deleted(payloads) == []
    assert state[FOREIGN_ITEM_KEY] == "old-hash"


def _failure(**kwargs: Any) -> Messager.Failures:  # noqa: ANN401
    return Messager.Failures(**kwargs)


@pytest.mark.parametrize(
    "failure",
    [
        _failure(temporary=True),
        _failure(permanent=True),
        _failure(key_temporary=["a"]),
        _failure(key_permanent=["a"]),
    ],
)
def test_main_publish_failure_on_batch_fails_run(
    parameters: dict[str, Any], mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch, failure: Messager.Failures
) -> None:
    """A failed publish must not be recorded as harvested: no deletes, state untouched, Job fails"""
    monkeypatch.setenv("ALLOW_UNSAFE_DELETES", "true")
    mocker.patch.object(stac_harvester, "minimum_message_entries", 1)
    _patch_listing(mocker, _items(["a", "b"]))
    previous = {ITEM_PREFIX + "a.json": "old-hash", ITEM_PREFIX + "stale.json": "old-hash"}

    payloads, state = _run_main(
        mocker, parameters, previous=previous, raises=stac_harvester.PublishError, consume_results=[failure]
    )

    # Both batches and the final flush were attempted, but the delete message never was
    assert [list(payload["harvested_data"]) for payload in payloads] == [
        [ITEM_PREFIX + "a.json"],
        [ITEM_PREFIX + "b.json"],
        [],
    ]
    assert _deleted(payloads) == []
    assert state == previous


def test_main_publish_failure_on_final_flush_fails_run(parameters: dict[str, Any], mocker: MockerFixture) -> None:
    _patch_listing(mocker, _items(["a"]))
    previous = {ITEM_PREFIX + "a.json": "old-hash", ITEM_PREFIX + "stale.json": "old-hash"}

    payloads, state = _run_main(
        mocker,
        parameters,
        previous=previous,
        raises=stac_harvester.PublishError,
        consume_results=[_failure(permanent=True)],
    )

    assert len(payloads) == 1
    assert _deleted(payloads) == []
    assert state == previous


def test_main_publish_failure_on_delete_message_fails_run(parameters: dict[str, Any], mocker: MockerFixture) -> None:
    """If the deletes were not delivered their keys must stay in the harvest metadata"""
    _patch_listing(mocker, _items(["a"]))
    previous = {ITEM_PREFIX + "a.json": "old-hash", ITEM_PREFIX + "stale.json": "old-hash"}

    payloads, state = _run_main(
        mocker,
        parameters,
        previous=previous,
        raises=stac_harvester.PublishError,
        consume_results=[_failure(), _failure(temporary=True)],
    )

    assert _deleted(payloads) == [ITEM_PREFIX + "stale.json"]
    assert state == previous


def test_send_messages_only_treats_reported_failures_as_failure() -> None:
    messager = mock.MagicMock()

    messager.consume.return_value = _failure()
    assert stac_harvester.send_messages(messager, {}) is True
    messager.consume.return_value = None
    assert stac_harvester.send_messages(messager, {}) is True
    messager.consume.return_value = mock.MagicMock()
    assert stac_harvester.send_messages(messager, {}) is True
    messager.consume.return_value = _failure(temporary=True)
    assert stac_harvester.send_messages(messager, {}) is False


@pytest.mark.parametrize(
    ("source_url", "listed_self"),
    [(COLLECTION_URL + "/", COLLECTION_URL), (COLLECTION_URL, COLLECTION_URL + "/")],
)
def test_prepare_catalogue_matches_collection_ignoring_trailing_slash(
    mocker: MockerFixture, source_url: str, listed_self: str
) -> None:
    _patch_collection_source(mocker, ["item-1"], listed_self=listed_self, source_url=source_url)
    run_state = stac_harvester.HarvestRunState()

    _, pages = stac_harvester.prepare_catalogue_stac(source_url, run_state)

    assert [page.get_self_href() for page in pages] == [ROOT_URL, listed_self, _own_item_url("item-1")]


@pytest.mark.parametrize(
    ("source_url", "listed_url"),
    [(COLLECTION_URL + "/", COLLECTION_URL), (COLLECTION_URL, COLLECTION_URL + "/")],
)
def test_main_owned_prefix_ignores_trailing_slash(
    parameters: dict[str, Any], mocker: MockerFixture, source_url: str, listed_url: str
) -> None:
    own_stale_key = OWN_ITEM_PREFIX + "item-gone.json"
    previous = {own_stale_key: "old-hash", FOREIGN_ITEM_KEY: "old-hash"}
    listing: Listing = [
        (listed_url, COLLECTION_KEY.removesuffix(".json")),
        (_item_url("item-1"), OWN_ITEM_PREFIX + "item-1"),
    ]
    _patch_listing(mocker, listing, collection_mode=True)

    payloads, state = _run_main(mocker, parameters, previous=previous, source_url=source_url)

    assert _deleted(payloads) == [own_stale_key]
    assert state[FOREIGN_ITEM_KEY] == "old-hash"


def test_get_file_contents_retries_bad_status_line(mocker: MockerFixture) -> None:
    mock_sleep = mocker.patch("stac_harvester.__main__.time.sleep")
    mock_urlopen = mocker.patch(
        "stac_harvester.__main__.urlopen",
        side_effect=[http.client.BadStatusLine("garbage"), _response(lambda: b"body")],
    )

    assert stac_harvester.get_file_contents(FETCH_URL) == b"body"
    assert mock_urlopen.call_count == 2
    assert mock_sleep.call_count == 1


@pytest.mark.parametrize("value", ["nan", "abc", "2", "-0.1", "inf", ""])
def test_main_rejects_invalid_max_delete_fraction(
    parameters: dict[str, Any], mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    """A bad fraction fails the run before anything is fetched, sent or stored"""
    monkeypatch.setenv("MAX_DELETE_FRACTION", value)
    mock_parse = _patch_listing(mocker, _items(["a"]))
    previous = _item_state(3)

    payloads, state = _run_main(mocker, parameters, previous=previous, raises=ValueError)

    assert payloads == []
    assert state == previous
    mock_parse.assert_not_called()
    pulsar_client: Any = stac_harvester.get_pulsar_client
    pulsar_client.assert_not_called()


@pytest.mark.parametrize(("value", "expected"), [(None, 0.2), ("0", 0.0), ("0.5", 0.5), ("1", 1.0)])
def test_get_max_delete_fraction(monkeypatch: pytest.MonkeyPatch, value: str | None, expected: float) -> None:
    if value is not None:
        monkeypatch.setenv("MAX_DELETE_FRACTION", value)

    assert stac_harvester.get_max_delete_fraction() == expected
