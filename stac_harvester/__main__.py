import argparse
import datetime
import hashlib
import http.client
import json
import logging
import math
import os
import time
import urllib.error
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from itertools import chain
from typing import Any
from urllib.parse import quote, urljoin
from urllib.request import urlopen

import requests
from botocore.client import BaseClient
from eodhp_utils.aws.s3 import get_file_s3, upload_file_s3
from eodhp_utils.messagers import Messager
from eodhp_utils.runner import get_boto3_session, get_pulsar_client, setup_logging
from opentelemetry import trace
from opentelemetry.baggage import set_baggage
from opentelemetry.context import attach, detach
from pystac import Catalog, Collection, Item
from pystac.errors import STACError, STACTypeError
from pystac.link import HREF
from pystac_client import Client as PystacClient
from pystac_client.conformance import ConformanceClasses
from pystac_client.errors import ClientTypeError
from pystac_client.exceptions import APIError
from pystac_client.stac_api_io import StacApiIO
from urllib3.util.retry import Retry

from .stac_harvester_messager import STACHarvesterMessager


class HarvesterFetchError(Exception):
    """A transient failure may be an outage of the source, so it counts towards aborting the harvest"""

    def __init__(self, message: str, *, transient: bool = True) -> None:
        super().__init__(message)
        self.transient = transient


class CollectionNotFoundError(Exception):
    pass


class PublishError(Exception):
    pass


parser = argparse.ArgumentParser()

parser.add_argument("source_url", help="URL of STAC catalog or collection to harvest", type=str)
parser.add_argument("workspace_name", help="Name of the workspace", type=str)
parser.add_argument("source", help="Path of the STAC catalog", type=str)
parser.add_argument("target", help="Updated path of the STAC catalog", type=str)
parser.add_argument("metadata_s3_bucket", help="S3 bucket to store harvested metadata", type=str)

setup_logging(verbosity=2, enable_otel_logging=True)

minimum_message_entries = int(os.environ.get("MINIMUM_MESSAGE_ENTRIES", 100))

FETCH_ATTEMPTS = 4
FETCH_TIMEOUT_SECONDS = 20
FETCH_BACKOFF_SECONDS = 2
MAX_CONSECUTIVE_FETCH_FAILURES = 5
MAX_TOTAL_FETCH_FAILURES = 50
DELETE_COUNT_FLOOR = 10
DEFAULT_MAX_DELETE_FRACTION = 0.2

global parent_keys

parent_keys: dict[str, str] = {}

tracer = trace.get_tracer(__name__)


@dataclass
class HarvestRunState:
    is_partial: bool = False
    collection_mode: bool = False
    collection_id: str | None = None
    owned_prefix: str | None = None


def get_file_contents(url: str) -> bytes:
    """Returns contents of file at a given URL, retrying transient failures with backoff"""

    last_error: Exception | None = None
    for attempt in range(FETCH_ATTEMPTS):
        if attempt:
            time.sleep(FETCH_BACKOFF_SECONDS * 2 ** (attempt - 1))
        try:
            with urlopen(url, timeout=FETCH_TIMEOUT_SECONDS) as response:
                return response.read()
        except urllib.error.HTTPError as e:
            # Must precede OSError. Client errors other than timeout/rate-limit will not recover.
            if 400 <= e.code < 500 and e.code not in (408, 429):
                raise HarvesterFetchError(
                    f"Failed to fetch {url}: HTTP {e.code}", transient=e.code not in (404, 410)
                ) from e
            last_error = e
        except (OSError, http.client.IncompleteRead, http.client.BadStatusLine) as e:
            last_error = e
        logging.warning(f"Unable to access {url} (attempt {attempt + 1}/{FETCH_ATTEMPTS}): {last_error}")
    raise HarvesterFetchError(f"Failed to fetch {url} after {FETCH_ATTEMPTS} attempts: {last_error}")


def fetch_json(url: str) -> tuple[bytes, dict[str, Any]]:
    """Returns the raw and decoded contents of the JSON object at a given URL"""
    body = get_file_contents(url)
    try:
        data = json.loads(body.decode("utf-8"))
    except ValueError as e:
        raise HarvesterFetchError(f"Undecodable response from {url}: {e}") from e
    if not isinstance(data, dict):
        raise HarvesterFetchError(f"Response from {url} is not a JSON object")
    return body, data


def get_file_hash(body: bytes) -> str:
    """Returns hash of data available at given URL"""

    def _md5_hash(byte_str: bytes) -> str:
        """Calculates an md5 hash for given bytestring"""
        md5 = hashlib.md5()
        md5.update(byte_str)
        return md5.hexdigest()

    return _md5_hash(body)


def get_metadata(bucket: str, key: str, s3_client: BaseClient) -> dict[str, Any]:
    """Read file at given S3 location and parse as JSON"""
    previously_harvested = get_file_s3(bucket, key, s3_client)
    try:
        previously_harvested = json.loads(previously_harvested)
    except TypeError:
        previously_harvested = {}
    return previously_harvested


@dataclass
class _ListingProgress:
    kind: str | None = None
    count: int = 0
    number_matched: int | None = None


class ListingValidatingStacApiIO(StacApiIO):
    """StacApiIO that rejects malformed or incomplete listings. pystac-client ends pagination silently
    on a page with no features/collections, which would make a truncated listing look complete"""

    _listing: _ListingProgress | None = None

    def read_json(self, source: HREF, *args: object, **kwargs: object) -> dict[str, Any]:
        page = super().read_json(source, *args, **kwargs)
        listing = self._listing
        if listing is None:
            return page

        is_first_page = listing.kind is None
        if is_first_page and isinstance(page, dict):
            listing.kind = next(
                (kind for kind in ("features", "collections") if isinstance(page.get(kind), list)), None
            )
        entries = page.get(listing.kind) if isinstance(page, dict) and listing.kind else None
        if not isinstance(entries, list):
            expected = listing.kind or "features or collections"
            raise APIError(f"Malformed listing page from {source}: expected a list of {expected}")

        has_next = any(isinstance(link, dict) and link.get("rel") == "next" for link in page.get("links") or [])
        if not entries and has_next:
            raise APIError(f"Incomplete listing from {source}: empty page with a next link")

        listing.count += len(entries)
        number_matched = page.get("numberMatched")
        if is_first_page and isinstance(number_matched, int) and not isinstance(number_matched, bool):
            listing.number_matched = number_matched
        if not has_next and listing.number_matched is not None and listing.count < listing.number_matched:
            raise APIError(
                f"Incomplete listing from {source}: ended after {listing.count} of {listing.number_matched} entries"
            )
        return page

    def get_pages(
        self, url: str, method: str | None = None, parameters: dict[str, Any] | None = None
    ) -> Iterator[dict[str, Any]]:
        pages = super().get_pages(url, method=method, parameters=parameters)
        listing = _ListingProgress()
        while True:
            # Only validate while pystac-client reads a page, not while the caller handles one
            self._listing = listing
            try:
                page = next(pages)
            except StopIteration:
                return
            finally:
                self._listing = None
            yield page


def _create_stac_io() -> StacApiIO:
    """Create a StacApiIO with retry and timeout support"""
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=None,
    )
    return ListingValidatingStacApiIO(max_retries=retry, timeout=FETCH_TIMEOUT_SECONDS)


def _create_stac_client(url: str) -> PystacClient:
    """Create a PystacClient with retry support"""
    # The timeout has to be passed here too, as PystacClient.open resets the one held by the StacApiIO
    return PystacClient.open(url, stac_io=_create_stac_io(), timeout=FETCH_TIMEOUT_SECONDS)


def is_same_url(url: str, other_url: str) -> bool:
    """Compares URLs ignoring any trailing slash"""
    return url.rstrip("/") == other_url.rstrip("/")


def iter_search_items_safely(client: PystacClient, run_state: HarvestRunState | None = None) -> Iterator[Item]:
    """Yield valid item-search results while skipping malformed STAC item payloads."""
    search = client.search()
    base_url = getattr(search, "url", None)
    for item_dict in search.items_as_dicts():
        try:
            if not isinstance(item_dict, dict):
                raise TypeError(f"Item-search feature must be an object, got {type(item_dict).__name__}")
            if isinstance(base_url, str):
                resolve_item_search_self_href(item_dict, base_url)
            yield Item.from_dict(item_dict, root=client, preserve_dict=False)
        except (AttributeError, KeyError, TypeError, ValueError, STACError, STACTypeError):
            if run_state is not None:
                run_state.is_partial = True
            item_id = get_item_id(item_dict)
            logging.exception(f"Skipping malformed STAC item {item_id}")
            continue


def resolve_item_search_self_href(item_dict: dict[str, Any], base_url: str) -> None:
    """Make relative item-search self hrefs absolute before PySTAC conversion."""
    for link in item_dict.get("links", []):
        if not isinstance(link, dict) or link.get("rel") != "self":
            continue

        href = link.get("href")
        if isinstance(href, str):
            link["href"] = urljoin(base_url if base_url.endswith("/") else base_url + "/", href)
        return


def prepare_catalogue_stac(source_url: str, run_state: HarvestRunState | None = None) -> tuple[str, Any]:
    """Returns all links to URLs that are children of the given source URL"""

    try:
        client = _create_stac_client(source_url)
        name = client.id

        catalogue = [client.get_root()]
        all_collections = client.get_collections()
        if client.conforms_to(ConformanceClasses.ITEM_SEARCH):
            all_items = iter_search_items_safely(client, run_state)
        else:
            all_items = client.get_all_items()
        pages: Any = chain(catalogue, all_collections, all_items)
    except ClientTypeError:
        collection = Collection.from_file(source_url, stac_io=_create_stac_io())
        name = collection.id
        if run_state is not None:
            run_state.collection_mode = True
            run_state.collection_id = name

        root_catalogue = next(link.target for link in collection.links if link.rel == "root")

        client = _create_stac_client(str(root_catalogue))
        all_collections = client.get_collections()
        pages_list: list[Any] = [client.get_root()]
        for collection in all_collections:
            self_link = next(link.target for link in collection.links if link.rel == "self")
            if collection.id == name and is_same_url(str(self_link), source_url):
                pages_list.append(collection)
                pages = chain(pages_list, collection.get_items(recursive=True))
                break
        else:
            raise CollectionNotFoundError(
                f"Collection {source_url} not found under root catalogue {root_catalogue}; refusing to harvest"
            ) from None

    return name, pages


def parse_github_url(github_url: str) -> tuple[str, str, str]:
    split_url = github_url.split("/")
    catalogue_name = split_url[-1].rstrip(".json")

    owner = split_url[3]
    repo = split_url[4]

    return owner, repo, catalogue_name


def prepare_catalogue_git(source_url: str) -> tuple[str, list[dict[str, Any]], str]:
    items: list[dict[str, Any]] = []
    owner, repo, catalogue_name = parse_github_url(source_url)
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/"

    response = requests.get(url)

    if response.status_code == 200:
        files = response.json()
        for file in files:
            data_url = file["download_url"]
            data_raw = get_file_contents(data_url)
            data = json.loads(data_raw.decode("utf-8"))
            data["harvester_file_name"] = file["name"]
            items.append(data)

    else:
        logging.error(f"Error: {response.status_code}")
        response.raise_for_status()

    return catalogue_name, items, repo


def get_type(data: dict[str, Any]) -> str:
    if data["type"] == "Catalog":
        return "catalogs"
    elif data["type"] == "Collection":
        return "collections"
    elif data["type"] == "Feature":
        return "items"
    else:
        logging.warning(f"Data type not recognised: {data['type']}")
        return ""


def get_parent_url(data: dict[str, Any]) -> str | None:
    for link in data.get("links", {}):
        if link.get("rel") == "parent":
            return link.get("href")

    return None


def search_git_items(url: str, items: list[dict[str, Any]]) -> dict[str, Any] | None:
    short_url = url.lstrip("/").rstrip(".json")
    for item in items:
        if item.get("harvester_file_name", "undefined").rstrip(".json") == short_url:
            return item
    return None


def parse_url_git(item: dict[str, Any], items: list[dict[str, Any]]) -> tuple[str, Any, str]:
    global parent_keys
    parent_keys = {}

    key = ""
    url = ""
    entry_url: str | None = None
    entry_data = item
    entry_data_raw = json.dumps(entry_data)
    file_hash = get_file_hash(entry_data_raw.encode("utf-8"))

    for link in entry_data.get("links", []):
        if link.get("rel") == "self":
            url = entry_url = link.get("href")

    while url:
        if url == entry_url:
            data: dict[str, Any] | None = entry_data
        else:
            data = search_git_items(url, items)

        if data is None:
            break

        entry_type = get_type(data)
        url = get_parent_url(data)

        if url and (parent_key := parent_keys.get(url)):
            key = parent_key + "/" + get_type(entry_data) + "/" + entry_data["id"]
            break

        key = entry_type + "/" + data["id"] + "/" + key

    key = key.rstrip("/")
    del entry_data["harvester_file_name"]

    if entry_data.get("type") in ["Catalog", "Collection"] and entry_url is not None:
        parent_keys[entry_url] = key

    return key, entry_data, file_hash


def parse_url_stac(entry_url: str) -> tuple[str, dict[str, Any], str]:
    global parent_keys

    key = ""
    entry_data_raw, entry_data = fetch_json(entry_url)
    file_hash = get_file_hash(entry_data_raw)

    url: str | None = entry_url
    while url:
        if url == entry_url:
            data = entry_data
        else:
            _, data = fetch_json(url)

        entry_type = get_type(data)
        if not entry_type:
            raise ValueError(f"Unrecognised STAC type {data['type']!r} at {url}")
        url = get_parent_url(data)

        if url and (parent_key := parent_keys.get(url)):
            key = parent_key + "/" + get_type(entry_data) + "/" + entry_data["id"]
            break

        key = entry_type + "/" + data["id"] + "/" + key

    key = key.rstrip("/")

    if entry_data["type"] in ["Catalog", "Collection"]:
        parent_keys[entry_url] = key

    return key, entry_data, file_hash


def make_git_catalogue_item(catalogue_name: str) -> list[dict[str, Any]]:
    catalogue: dict[str, Any] = {
        "type": "Catalog",
        "id": catalogue_name,
        "title": catalogue_name,
        "description": "Harvested Git catalogue",
        "stac_version": "1.0.0",
        "links": [],
        "stac_extensions": [],
    }

    catalogue["harvester_file_name"] = f"{catalogue_name}.json"

    return [catalogue]


def get_item_id(item: Item | dict[str, Any]) -> str:
    try:
        if isinstance(item, dict):
            return item.get("harvester_file_name") or item.get("id", "<unknown>")
        else:
            return item.get_self_href() or "<unknown>"
    except Exception:
        return "<unknown>"


def validate_fetched_entry(item: object, data: dict[str, Any]) -> None:
    """Raises ValueError if the JSON fetched for a listed STAC object is not that object"""
    if isinstance(item, Collection):
        expected_type = "Collection"
    elif isinstance(item, Catalog):
        expected_type = "Catalog"
    elif isinstance(item, Item):
        expected_type = "Feature"
    else:
        return

    if data.get("type") != expected_type or data.get("id") != item.id:
        raise ValueError(
            f"Fetched entry does not match listed {expected_type} {item.id!r}: "
            f"got type {data.get('type')!r}, id {data.get('id')!r}"
        )


def validate_entry_key(key: str, data: dict[str, Any], run_state: HarvestRunState) -> None:
    """Raises ValueError if an item's key is not beneath a catalogue or collection or, when harvesting
    a single collection, not beneath that collection"""
    if data.get("type") != "Feature":
        return

    if run_state.collection_mode:
        valid = run_state.owned_prefix is not None and key.startswith(run_state.owned_prefix + "/")
    else:
        valid = not key.startswith("items/")
    if not valid:
        raise ValueError(f"Item key {key} is not beneath {run_state.owned_prefix or 'a catalogue or collection'}")


def get_allow_unsafe_deletes() -> bool:
    """Reads ALLOW_UNSAFE_DELETES"""
    return os.environ.get("ALLOW_UNSAFE_DELETES", "").strip().lower() in ("1", "true", "yes")


def get_max_delete_fraction() -> float:
    """Reads and validates MAX_DELETE_FRACTION"""
    raw_value = os.environ.get("MAX_DELETE_FRACTION")
    if raw_value is None:
        return DEFAULT_MAX_DELETE_FRACTION
    try:
        value = float(raw_value)
    except ValueError:
        value = math.nan
    if not (math.isfinite(value) and 0 <= value <= 1):
        raise ValueError(f"MAX_DELETE_FRACTION must be a number between 0 and 1, got {raw_value!r}")
    return value


def send_messages(messager: STACHarvesterMessager, messages: dict[str, Any]) -> bool:
    """Sends harvested data and deleted keys. Returns False if any of it failed to be published"""
    failures = messager.consume(messages)
    if isinstance(failures, Messager.Failures) and (failures.any_permanent() or failures.any_temporary()):
        logging.error(f"PUBLISH FAILED: {failures}")
        return False
    return True


def is_item_key(key: str) -> bool:
    """Returns True for keys of the form .../items/<id>.json; catalogues and collections are not items"""
    parts = key.split("/")
    return len(parts) >= 2 and parts[-2] == "items"


def split_stale_keys(stale_keys: list[str], run_state: HarvestRunState) -> tuple[list[str], list[str]]:
    """Splits stale keys into (deletable, out_of_scope). A collection harvest only owns keys beneath
    its own collection, so that collection, the shared root catalogue and other collections are never
    deletable"""
    if not run_state.collection_mode:
        return list(stale_keys), []

    prefix = run_state.owned_prefix
    if prefix is None:
        return [], list(stale_keys)

    deletable: list[str] = []
    out_of_scope: list[str] = []
    for key in stale_keys:
        if key.startswith(prefix + "/"):
            deletable.append(key)
        else:
            out_of_scope.append(key)
    return deletable, out_of_scope


def get_delete_block_reason(
    deleted_keys: list[str], previous_keys: Iterable[str], latest_keys: Iterable[str], max_delete_fraction: float
) -> str | None:
    """Delete circuit breaker. Returns the reason the given deletes must not be sent, or None if
    they are safe to send"""
    if not deleted_keys:
        return None

    containers = [key for key in deleted_keys if not is_item_key(key)]
    if containers:
        return f"would delete {len(containers)} catalogue/collection keys: {containers[:20]}"

    previous = list(previous_keys)
    previous_item_count = sum(1 for key in previous if is_item_key(key))
    if previous_item_count and not any(is_item_key(key) for key in latest_keys):
        return f"listing returned no items but {previous_item_count} were previously harvested"

    if len(deleted_keys) > DELETE_COUNT_FLOOR and len(deleted_keys) > max_delete_fraction * len(previous):
        return f"would delete {len(deleted_keys)} of {len(previous)} previously harvested keys"

    return None


def main() -> None:
    """Harvest a given upstream STAC catalog, and all records beneath it. Send a pulsar message
    containing all added, updated, and deleted links since the last time the catalog was
    harvested"""

    s3_client = get_boto3_session().client("s3")

    args = parser.parse_args()
    max_delete_fraction = get_max_delete_fraction()
    allow_unsafe_deletes = get_allow_unsafe_deletes()

    token_workspace = attach(set_baggage("workspace", args.workspace_name))
    token_source = attach(set_baggage("source_url", args.source_url))

    topic = os.getenv("TOPIC")
    if topic:
        identifier = "_" + topic
    else:
        identifier = ""

    pulsar_client = get_pulsar_client()

    producer = pulsar_client.create_producer(
        topic=f"harvested{identifier}",
        producer_name=f"stac_harvester/{args.source_url}_{datetime.datetime.now()}",
        chunking_enabled=True,
    )

    logging.info(f"Harvesting from source URL: {args.source_url}")

    metadata_s3_key = f"harvested-metadata/{args.source_url.replace('/', '')}"
    previous_metadata = get_metadata(args.metadata_s3_bucket, metadata_s3_key, s3_client)
    previously_harvested = previous_metadata.copy()
    logging.info(f"Previously harvested URLs: {previously_harvested}")
    latest_harvested: dict[str, Any] = {}
    harvested_data: dict[str, Any] = {}

    items: Any
    run_state = HarvestRunState()
    if "github.com" in args.source_url:
        simplified_url = quote(args.source_url).replace("/", "%2f")
        output_prefix = f"git-harvester/{simplified_url}/"
        _catalogue_name, items, repo = prepare_catalogue_git(args.source_url)
        is_git = True
        source = simplified_url
        items = make_git_catalogue_item(repo) + items
        key_prefix = repo
    else:
        output_prefix = f"stac-harvester/{args.source}/"
        _catalogue_name, items = prepare_catalogue_stac(args.source_url, run_state)
        is_git = False
        source = args.source
        key_prefix = ""

    messages: dict[str, Any] = {"source": source, "target": args.target}

    stac_harvester_messager = STACHarvesterMessager(
        s3_client=s3_client,
        output_bucket=args.metadata_s3_bucket,
        cat_output_prefix=output_prefix,
        producer=producer,
    )
    # Start OpenTelemetry trace (ensures logs have trace context)
    with tracer.start_as_current_span("stac_harvester.run"):
        # Pulsar messages are sent once for every 100 entities
        iterator_failed = False
        publish_failed = False
        consecutive_fetch_failures = 0
        total_fetch_failures = 0
        try:
            for item in items:
                try:
                    if is_git:
                        # logging.info(f"Parsing item: {item['harvester_file_name']}")
                        key, data, file_hash = parse_url_git(item, items)
                    else:
                        url = item.get_self_href()

                        # logging.info(f"Parsing URL: {url}")
                        key, data, file_hash = parse_url_stac(url)
                        validate_fetched_entry(item, data)
                        validate_entry_key(key, data, run_state)

                        # The collection at the configured URL is the collection this harvester owns
                        if (
                            run_state.collection_mode
                            and run_state.owned_prefix is None
                            and data.get("type") == "Collection"
                            and data.get("id") == run_state.collection_id
                            and is_same_url(url, args.source_url)
                        ):
                            run_state.owned_prefix = key

                    consecutive_fetch_failures = 0

                    if key == "" and is_git:
                        key = key_prefix + key + ".json"
                    elif is_git:
                        key = key_prefix + "/" + key + ".json"
                    else:
                        key = key_prefix + key + ".json"

                    previous_hash = previously_harvested.pop(key, None)
                    latest_harvested[key] = file_hash

                    if not previous_hash or previous_hash != file_hash:
                        # Data was not harvested previously
                        logging.info(f"Added: {key}")
                        harvested_data[key] = data
                    else:
                        logging.info(f"Skipping: {key}")

                    if len(harvested_data) >= minimum_message_entries:
                        messages["harvested_data"] = harvested_data
                        messages["deleted_keys"] = []

                        # logging.info(f"Sending {len(harvested_data)} messages: {messages}")
                        if not send_messages(stac_harvester_messager, messages):
                            publish_failed = True
                            break
                        harvested_data = {}
                except HarvesterFetchError as e:
                    run_state.is_partial = True
                    item_id = get_item_id(item)
                    logging.error(f"Skipping item {item_id} due to fetch failure: {e}")
                    if not e.transient:
                        continue
                    consecutive_fetch_failures += 1
                    total_fetch_failures += 1
                    if (
                        consecutive_fetch_failures >= MAX_CONSECUTIVE_FETCH_FAILURES
                        or total_fetch_failures >= MAX_TOTAL_FETCH_FAILURES
                    ):
                        logging.error(
                            f"Aborting harvest: source appears to be down ({consecutive_fetch_failures} "
                            f"consecutive, {total_fetch_failures} total fetch failures)"
                        )
                        break
                    continue
                except Exception:
                    run_state.is_partial = True
                    item_id = get_item_id(item)
                    logging.exception(f"Skipping item {item_id} due to unexpected error")
                    continue
        except (
            ConnectionError,
            OSError,
            AttributeError,
            KeyError,
            TypeError,
            ValueError,
            STACError,
            STACTypeError,
            APIError,
        ):
            iterator_failed = run_state.is_partial = True
            logging.exception("Iterator failed during pagination or item construction. Flushing harvested data.")

        messages["harvested_data"] = harvested_data
        messages["deleted_keys"] = []
        if publish_failed or not send_messages(stac_harvester_messager, messages):
            # Nothing is known to have been delivered, so everything must be re-sent by the next run
            raise PublishError("Failed to publish harvested data; no deletes sent and harvest metadata left untouched")

        stale_keys = list(previously_harvested.keys())
        if not iterator_failed and not run_state.is_partial:
            deleted_keys, retained_keys = split_stale_keys(stale_keys, run_state)
            if retained_keys:
                logging.warning(
                    f"Not deleting {len(retained_keys)} stale keys outside owned prefix "
                    f"{run_state.owned_prefix}: {retained_keys[:20]}"
                )

            if is_git or allow_unsafe_deletes:
                block_reason = None
            else:
                block_reason = get_delete_block_reason(
                    deleted_keys, previous_metadata.keys(), latest_harvested.keys(), max_delete_fraction
                )
            if block_reason:
                logging.error(
                    f"DELETE CIRCUIT BREAKER: {block_reason}. No deletes sent; set ALLOW_UNSAFE_DELETES=true "
                    "on a one-off run to override."
                )
                retained_keys += deleted_keys
                deleted_keys = []

            if deleted_keys:
                # Sent on their own, once every update is known to have been published
                messages["harvested_data"] = {}
                messages["deleted_keys"] = deleted_keys
                if not send_messages(stac_harvester_messager, messages):
                    raise PublishError("Failed to publish deletes; harvest metadata left untouched")
            logging.info(f"Harvested files: {latest_harvested}")
        else:
            retained_keys = stale_keys
            logging.error(
                f"HARVEST PARTIAL: suppressing deletions; recording {len(latest_harvested)} harvested keys and "
                f"retaining {len(retained_keys)} previously harvested keys in harvest metadata."
            )

        # A key only leaves the harvest metadata once a delete has actually been sent for it.
        new_state = latest_harvested | {key: previously_harvested[key] for key in retained_keys}
        upload_file_s3(json.dumps(new_state), args.metadata_s3_bucket, metadata_s3_key, s3_client)

    detach(token_source)
    detach(token_workspace)


if __name__ == "__main__":
    main()
