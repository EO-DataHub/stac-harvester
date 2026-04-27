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
from pystac.errors import STACError
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


def test_iter_search_items_safely_does_not_catch_consumer_exceptions() -> None:
    """Downstream item processing errors must not be reclassified as malformed conversion errors."""
    client = mock.MagicMock()
    run_state = stac_harvester.HarvestRunState()
    search = mock.MagicMock()
    search.url = "https://example.com/search"
    search.items_as_dicts.return_value = [
        {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "valid-item",
            "geometry": None,
            "bbox": None,
            "properties": {"datetime": "2020-01-01T00:00:00Z"},
            "links": [{"rel": "self", "href": "https://example.com/items/valid-item"}],
        }
    ]
    client.search.return_value = search
    iterator = stac_harvester.iter_search_items_safely(client, run_state)

    item = next(iterator)
    assert item.id == "valid-item"

    with pytest.raises(RuntimeError):
        raise RuntimeError("consumer failed")
    assert run_state.is_partial is False


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
    parameters: dict[str, Any], mocker: MockerFixture
) -> None:
    """Malformed item-search features make the run partial, so stale keys cannot be deleted."""
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
def test_main_flushes_on_malformed_iterator_item(parameters: dict[str, Any], mocker: MockerFixture) -> None:
    """If item construction fails outside the loop body, flush processed data and avoid deletions."""
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
    assert stored_metadata == previous_metadata
