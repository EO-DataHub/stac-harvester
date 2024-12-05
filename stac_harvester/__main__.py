import argparse
import hashlib
import json
import logging
import os
import urllib.request
from itertools import chain
from urllib.request import urlopen

import boto3
from botocore.exceptions import ClientError
from pulsar import Client as PulsarClient
from pystac import Collection
from pystac_client import Client as PystacClient
from pystac_client.errors import ClientTypeError

from .stac_harvester_messager import STACHarvesterMessager

parser = argparse.ArgumentParser()
parser.add_argument("source_url", help="URL of STAC catalog or collection to harvest", type=str)
parser.add_argument("workspace_name", help="Name of the workspace", type=str)
parser.add_argument("catalog", help="Path of the STAC catalog", type=str)
parser.add_argument("metadata_s3_bucket", help="S3 bucket to store harvested metadata", type=str)
parser.add_argument("files_s3_bucket", help="S3 bucket to store harvested files", type=str)


logging.basicConfig(level=logging.DEBUG, format="%(message)s")


def upload_file_s3(body: str, bucket: str, key: str, s3_client: boto3.client):
    """Upload data to an S3 bucket"""
    try:
        s3_client.put_object(Body=body, Bucket=bucket, Key=key)
    except ClientError as e:
        logging.error(f"File upload failed: {e}")


def get_file_s3(bucket: str, key: str, s3_client: boto3.client) -> str:
    """Retrieve data from an S3 bucket"""
    try:
        file_obj = s3_client.get_object(Bucket=bucket, Key=key)
        return file_obj["Body"].read().decode("utf-8")
    except ClientError as e:
        logging.error(f"File retrieval failed: {e}")
        return None


def get_file_hash(url: str, retries: int = 0) -> str:
    """Returns hash of data available at given URL"""

    def _md5_hash(byte_str: bytes) -> str:
        """Calculates an md5 hash for given bytestring"""
        md5 = hashlib.md5()
        md5.update(byte_str)
        return md5.hexdigest()

    if retries == 3:
        # Max number of retries
        return None
    try:
        with urlopen(url, timeout=5) as response:
            body = response.read()
    except urllib.error.URLError:
        logging.error(f"Unable to access {url}, retrying...")
        return get_file_hash(url, retries + 1)
    return _md5_hash(body)


def get_metadata(bucket: str, key: str, s3_client: boto3.client) -> dict:
    """Read file at given S3 location and parse as JSON"""
    previously_harvested = get_file_s3(bucket, key, s3_client)
    try:
        previously_harvested = json.loads(previously_harvested)
    except TypeError:
        previously_harvested = {}
    return previously_harvested


def find_urls(source_url: str) -> set:
    """Returns all links to URLs that are children of the given source URL"""
    urls = {source_url}

    try:
        client = PystacClient.open(source_url)
        all_items = client.get_all_items()
    except ClientTypeError:
        collection = Collection.from_file(source_url)
        root_catalogue = [link.target for link in collection.links if link.rel == "root"][0]

        client = PystacClient.open(root_catalogue)
        all_collections = client.get_collections()
        all_items = []
        for collection in all_collections:
            self_link = [link.target for link in collection.links if link.rel == "self"][0]
            if self_link == source_url:
                collection = client.get_collection(collection.id)
                all_items = chain(all_items, collection.get_items(recursive=True))

    for item in all_items:
        for link in item.links:
            if target := link.target:
                if link.rel in {
                    "child",
                    "items",
                    "item",
                    "self",
                    "parent",
                    "collection",
                } and is_valid_url(target, source_url):
                    urls.add(target)

    return urls


def is_valid_url(url: str, source_url: str) -> bool:
    """Verify URL is a valid link for harvesting"""
    if url and url.startswith(source_url) and not any(x in url for x in ["?", "thumbnail"]):
        return True
    return False


def generate_target(catalog: str, source_url: str) -> str:
    """Generate target string for harvested links"""

    # Ensure target and source are aligned with trailing slash, if provided in source
    if source_url.endswith("/"):
        catalog += "/"
    return catalog


def main():
    """Harvest a given upstream STAC catalog, and all records beneath it. Send a pulsar message
    containing all added, updated, and deleted links since the last time the catalog was
    harvested"""

    if os.getenv("AWS_ACCESS_KEY") and os.getenv("AWS_SECRET_ACCESS_KEY"):
        session = boto3.session.Session(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )
        s3_client = session.client("s3")
    else:
        s3_client = boto3.client("s3")

    args = parser.parse_args()

    updated_urls = []

    pulsar_url = os.environ.get("PULSAR_URL")
    pulsar_client = PulsarClient(pulsar_url)

    producer = pulsar_client.create_producer(
        topic="harvested", producer_name=f"stac_harvester/{args.source_url}", chunking_enabled=True
    )

    target = generate_target(args.catalog, args.source_url)

    stac_harvester = STACHarvesterMessager(
        s3_client=s3_client,
        output_bucket=args.files_s3_bucket,
        cat_output_prefix="stac-harvester",
        producer=producer,
    )

    print(stac_harvester)

    logging.info(f"Harvesting from source URL: {args.source_url}")

    metadata_s3_key = f"harvested-metadata/{args.source_url.replace('/', '')}"
    previously_harvested = get_metadata(args.metadata_s3_bucket, metadata_s3_key, s3_client)
    logging.info(f"Previously harvested URLs: {previously_harvested}")
    latest_harvested = {}

    url_list = find_urls(args.source_url)

    for url in url_list:
        logging.info(f"Parsing URL: {url}")
        file_hash = get_file_hash(url)
        previous_hash = previously_harvested.pop(url, None)
        if not previous_hash or previous_hash != file_hash:
            # URL was not harvested previously or has been updated
            logging.info("Appended URL to 'updated' list")
            updated_urls.append(url)
            msg = {
                "updated": True,
                "url": url,
                "source": args.source_url,
                "target": target,
            }
            stac_harvester.consume(msg)
        latest_harvested[url] = file_hash

    deleted_urls = list(previously_harvested.keys())
    for url in deleted_urls:
        logging.info(f"Previously harvested URLs not found: {deleted_urls}")
        msg = {
            "updated": False,
            "url": url,
            "source": args.source_url,
            "target": generate_target(args.catalog, args.source_url),
        }
        stac_harvester.consume(msg)

    # Record harvested hash data in S3
    upload_file_s3(
        json.dumps(latest_harvested), args.metadata_s3_bucket, metadata_s3_key, s3_client
    )
    logging.info(f"Harvested files: {latest_harvested}")


if __name__ == "__main__":
    main()
