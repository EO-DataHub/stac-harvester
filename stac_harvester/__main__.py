import argparse
import datetime
import hashlib
import json
import logging
import os
from urllib.error import HTTPError

import requests
import urllib.request
from itertools import chain
from urllib.parse import quote
from urllib.request import urlopen

import boto3
from eodhp_utils.aws.s3 import get_file_s3, upload_file_s3
from eodhp_utils.runner import get_boto3_session, get_pulsar_client, setup_logging
from pystac import Collection, Item, read_dict
from pystac_client import Client as PystacClient
from pystac_client.errors import ClientTypeError

from .stac_harvester_messager import STACHarvesterMessager

parser = argparse.ArgumentParser()

parser.add_argument("source_url", help="URL of STAC catalog or collection to harvest", type=str)
parser.add_argument("workspace_name", help="Name of the workspace", type=str)
parser.add_argument("source", help="Path of the STAC catalog", type=str)
parser.add_argument("target", help="Updated path of the STAC catalog", type=str)
parser.add_argument("metadata_s3_bucket", help="S3 bucket to store harvested metadata", type=str)


setup_logging(verbosity=2)

minimum_message_entries = int(os.environ.get("MINIMUM_MESSAGE_ENTRIES", 100))

global parent_keys

parent_keys = {}


def get_file_contents(url: str, retries: int = 0) -> bytes:
    """Returns hash of data available at given URL"""

    if retries == 3:
        # Max number of retries
        return None
    try:
        with urlopen(url, timeout=5) as response:
            body = response.read()
    except urllib.error.URLError:
        logging.error(f"Unable to access {url}, retrying...")
        return get_file_contents(url, retries + 1)
    return body


def get_file_hash(body: str) -> str:
    """Returns hash of data available at given URL"""

    def _md5_hash(byte_str: bytes) -> str:
        """Calculates an md5 hash for given bytestring"""
        md5 = hashlib.md5()
        md5.update(byte_str)
        return md5.hexdigest()

    return _md5_hash(body)


def get_metadata(bucket: str, key: str, s3_client: boto3.client) -> dict:
    """Read file at given S3 location and parse as JSON"""
    previously_harvested = get_file_s3(bucket, key, s3_client)
    try:
        previously_harvested = json.loads(previously_harvested)
    except TypeError:
        previously_harvested = {}
    return previously_harvested


def prepare_catalogue_stac(source_url: str) -> tuple:
    """Returns all links to URLs that are children of the given source URL"""

    try:
        client = PystacClient.open(source_url)
        name = client.id

        catalogue = [client.get_root()]
        all_collections = client.get_collections()
        all_items = client.get_all_items()
        pages = chain(catalogue, all_collections, all_items)
    except ClientTypeError:
        collection = Collection.from_file(source_url)
        name = collection.id

        root_catalogue = [link.target for link in collection.links if link.rel == "root"][0]

        client = PystacClient.open(root_catalogue)
        all_collections = client.get_collections()
        pages = []
        for collection in all_collections:
            self_link = [link.target for link in collection.links if link.rel == "self"][0]
            if self_link == source_url:
                pages.append(collection)
                collection = client.get_collection(collection.id)
                pages = chain(pages, collection.get_items(recursive=True))

    return name, pages


def parse_github_url(github_url):
    split_url = github_url.split("/")
    catalogue_name = split_url[-1].rstrip(".json")

    owner = split_url[3]
    repo = split_url[4]

    return owner, repo, catalogue_name


def prepare_catalogue_git(source_url: str) -> tuple:
    items = []
    owner, repo, catalogue_name = parse_github_url(source_url)
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/"

    response = requests.get(url)

    # client = PystacClient(id=catalogue_name, description="GitHub Catalogue")

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


def get_type(data: dict) -> str:
    if data["type"] == "Catalog":
        return "catalogs"
    elif data["type"] == "Collection":
        return "collections"
    elif data["type"] == "Feature":
        return "items"
    else:
        print(f"Data type not recognised: {data['type']}")


def get_parent_url(data: dict) -> str:

    for link in data["links"]:
        if link["rel"] == "parent":
            return link["href"]

    return None


def search_git_items(url, items):
    short_url = url.lstrip("/").rstrip(".json")
    for item in items:
        if item.get("harvester_file_name", "undefined").rstrip(".json") == short_url:
            return item


def parse_url_git(item: Item, items: list) -> tuple:
    global parent_keys
    parent_keys = {}

    key = ""
    url = ""
    entry_url = None
    entry_data = item
    entry_data_raw = json.dumps(entry_data)
    file_hash = get_file_hash(entry_data_raw.encode("utf-8"))

    for link in entry_data.get("links"):
        if link.get("rel") == "self":
            url = entry_url = link.get("href")

    while url:
        if url == entry_url:
            data = entry_data
        else:
            data = search_git_items(url, items)

        if data is None:
            break

        entry_type = get_type(data)
        url = get_parent_url(data)

        if parent_key := parent_keys.get(url):
            key = parent_key + "/" + get_type(entry_data) + "/" + entry_data["id"]
            break

        key = entry_type + "/" + data["id"] + "/" + key

    key = key.rstrip("/")
    del entry_data["harvester_file_name"]

    if entry_data.get("type") in ["Catalog", "Collection"] and not entry_url is None:
        parent_keys[entry_url] = key

    return key, entry_data, file_hash


def parse_url_stac(entry_url: str) -> tuple:
    global parent_keys

    key = ""
    entry_data_raw = get_file_contents(entry_url)
    entry_data = json.loads(entry_data_raw.decode("utf-8"))
    file_hash = get_file_hash(entry_data_raw)

    url = entry_url
    while url:
        if url == entry_url:
            data = entry_data
        else:
            data_raw = get_file_contents(url)
            data = json.loads(data_raw.decode("utf-8"))

        entry_type = get_type(data)
        url = get_parent_url(data)

        if parent_key := parent_keys.get(url):
            key = parent_key + "/" + get_type(entry_data) + "/" + entry_data["id"]
            break

        key = entry_type + "/" + data["id"] + "/" + key

    key = key.rstrip("/")

    if entry_data["type"] in ["Catalog", "Collection"]:
        parent_keys[entry_url] = key

    return key, entry_data, file_hash


def make_git_catalogue_item(catalogue_name):

    catalogue = {
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


def main():
    """Harvest a given upstream STAC catalog, and all records beneath it. Send a pulsar message
    containing all added, updated, and deleted links since the last time the catalog was
    harvested"""

    s3_client = get_boto3_session().client("s3")

    args = parser.parse_args()

    if os.getenv("TOPIC"):
        identifier = "_" + os.getenv("TOPIC")
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
    previously_harvested = get_metadata(args.metadata_s3_bucket, metadata_s3_key, s3_client)
    logging.info(f"Previously harvested URLs: {previously_harvested}")
    latest_harvested = {}
    harvested_data = {}

    if "github.com" in args.source_url:
        simplified_url = quote(args.source_url).replace("/", "%2f")
        output_prefix = f"git-harvester/{simplified_url}/"
        catalogue_name, items, repo = prepare_catalogue_git(args.source_url)
        is_git = True
        source = simplified_url
        items = make_git_catalogue_item(repo) + items
        key_prefix = repo
    else:
        output_prefix = f"stac-harvester/{args.source}/"
        catalogue_name, items = prepare_catalogue_stac(args.source_url)
        is_git = False
        source = args.source
        key_prefix = ""

    messages = {"source": source, "target": args.target}

    stac_harvester_messager = STACHarvesterMessager(
        s3_client=s3_client,
        output_bucket=args.metadata_s3_bucket,
        cat_output_prefix=output_prefix,
        producer=producer,
    )

    # Pulsar messages are sent once for every 100 entities
    for item in items:
        if is_git:
            logging.info(f"Parsing item: {item['harvester_file_name']}")
            key, data, file_hash = parse_url_git(item, items)
        else:
            url = item.get_self_href()

            logging.info(f"Parsing URL: {url}")
            key, data, file_hash = parse_url_stac(url)

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

            logging.info(f"Sending {len(harvested_data)} messages: {messages}")
            stac_harvester_messager.consume(messages)
            upload_file_s3(
                json.dumps(latest_harvested), args.metadata_s3_bucket, metadata_s3_key, s3_client
            )
            harvested_data = {}

    messages["harvested_data"] = harvested_data
    messages["deleted_keys"] = []
    stac_harvester_messager.consume(messages)
    upload_file_s3(
        json.dumps(latest_harvested), args.metadata_s3_bucket, metadata_s3_key, s3_client
    )

    deleted_keys = list(previously_harvested.keys())
    messages["harvested_data"] = harvested_data
    messages["deleted_keys"] = deleted_keys
    logging.info(
        f"Sending message with {len(harvested_data.keys())} entries and {len(deleted_keys)} deleted keys"
    )
    stac_harvester_messager.consume(messages)

    # Record harvested hash data in S3
    upload_file_s3(
        json.dumps(latest_harvested), args.metadata_s3_bucket, metadata_s3_key, s3_client
    )
    logging.info(f"Harvested files: {latest_harvested}")


if __name__ == "__main__":
    main()
