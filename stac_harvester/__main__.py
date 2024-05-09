import argparse
import hashlib
import json
import logging
import os
import urllib.request
from json import JSONDecodeError
from typing import List, Tuple
from urllib.request import urlopen

import boto3
from botocore.exceptions import ClientError
from pulsar import Client

parser = argparse.ArgumentParser()
parser.add_argument("source_url", help="URL of STAC catalog or collection to harvest", type=str)
parser.add_argument("workspace_name", help="Name of the workspace", type=str)
parser.add_argument("sub_catalog", help="Name of the STAC sub catalog", type=str)
parser.add_argument("metadata_s3_bucket", help="S3 bucket to store harvested metadata", type=str)
parser.add_argument("limit", help="Number of entries to return", type=str, default="10")

pulsar_url = os.environ.get("PULSAR_URL")
pulsar_client = Client(pulsar_url)

if os.getenv("AWS_ACCESS_KEY") and os.getenv("AWS_SECRET_ACCESS_KEY"):
    session = boto3.session.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    s3_client = session.client("s3")
else:
    s3_client = boto3.client("s3")

logging.basicConfig(level=logging.DEBUG, format="%(message)s")


def upload_file_s3(body: str, bucket: str, key: str):
    """Upload data to an S3 bucket"""
    try:
        s3_client.put_object(Body=body, Bucket=bucket, Key=key)
    except ClientError as e:
        logging.error(f"File upload failed: {e}")


def get_file_s3(bucket: str, key: str) -> str:
    """Retrieve data from an S3 bucket"""
    try:
        file_obj = s3_client.get_object(Bucket=bucket, Key=key)
        return file_obj["Body"].read().decode("utf-8")
    except ClientError as e:
        logging.error(f"File retrieval failed: {e}")
        return None


def get_file_contents(url: str, retries: int = 0) -> Tuple[str, str]:
    """Returns contents and hash of data available at given URL"""

    def _md5_hash(input: bytes) -> str:
        """Calculates an md5 hash for given bytestring"""
        md5 = hashlib.md5()
        md5.update(input)
        return md5.hexdigest()

    if retries == 3:
        # Max number of retries
        return None, None
    try:
        with urlopen(url, timeout=5) as response:
            body = response.read()
    except urllib.error.URLError:
        logging.error(f"Unable to access {url}, retrying...")
        return get_file_contents(url, retries + 1)
    return body.decode("utf-8"), _md5_hash(body)


def get_metadata(bucket: str, key: str) -> dict:
    """Read file at given S3 location and parse as JSON"""
    previously_harvested = get_file_s3(bucket, key)
    try:
        previously_harvested = json.loads(previously_harvested)
    except TypeError:
        previously_harvested = {}
    return previously_harvested


def find_urls(
    file_contents: str,
    source_url: str,
    urls_to_parse: List[str],
    parsed_urls: List[str],
    limit: int,
) -> List[str]:
    """Returns all links to URLs that are children of the given file_contents"""
    links = []

    def _decode_dict(a_dict):
        try:
            for link in a_dict["links"]:
                links.append(link)
        except KeyError:
            pass
        return a_dict

    try:
        json.loads(file_contents, object_hook=_decode_dict)
    except JSONDecodeError:
        return []
    new_urls = []
    for link in links:
        href = link.get("href")
        if (
            href not in new_urls
            and link.get("rel") in {"child", "items", "item", "self"}
            and check_add_url(href, source_url, urls_to_parse, parsed_urls)
        ):
            if link.get("rel") in {"items", "item"}:
                href = href.rstrip("/") + f"?limit={limit}"
            new_urls.append(href)
    logging.info(f"Found child URLs: {new_urls}")
    return new_urls


def check_add_url(
    url: str, source_url: str, urls_to_parse: List[str], parsed_urls: List[str]
) -> bool:
    """Verify URL is valid and has not been processed before"""
    if not (url in urls_to_parse or url in parsed_urls) and is_valid_url(url, source_url):
        return True
    return False


def is_valid_url(url: str, source_url: str) -> bool:
    """Verify URL is a valid link for harvesting"""
    if url and url.startswith(source_url) and not any(x in url for x in ["?", "thumbnail"]):
        return True
    return False


def main():
    """Harvest a given upstream STAC catalog, and all records beneath it. Send a pulsar message
    containing all added, updated, and deleted links since the last time the catalog was
    harvested"""
    args = parser.parse_args()
    added_urls = []
    updated_urls = []
    url_list = [args.source_url]
    parsed_url_list = []
    producer = pulsar_client.create_producer(topic="harvested", producer_name="stac_harvester")
    logging.info(f"Harvesting from source URL: {args.source_url}")

    metadata_s3_key = f"harvested-metadata/{args.source_url.replace('/', '')}"
    previously_harvested = get_metadata(args.metadata_s3_bucket, metadata_s3_key)
    logging.info(f"Previously harvested URLs: {previously_harvested}")
    latest_harvested = {}

    while url_list:
        url = url_list.pop(0)
        logging.info(f"Parsing URL: {url}")
        file_contents, file_hash = get_file_contents(url)
        if not file_contents:
            continue

        previous_hash = previously_harvested.pop(url, None)
        if not previous_hash:
            # URL was not harvested previously
            logging.info("Appended URL to 'added' list")
            added_urls.append(url)
        elif previous_hash != file_hash:
            # URL has changed since last run
            logging.info("Appended URL to 'updated' list")
            updated_urls.append(url)
        latest_harvested[url] = file_hash
        parsed_url_list.append(url)

        # Add all valid child links to the URL stack
        new_urls = find_urls(
            file_contents, args.source_url, url_list, parsed_url_list, args.limit
        )
        url_list += new_urls

    deleted_urls = list(previously_harvested.keys())
    logging.info(f"Previously harvested URLs not found: {deleted_urls}")

    output_data = {
        "id": f"{args.workspace_name}/{args.source_url}",
        "workspace": args.workspace_name,
        "bucket_name": args.metadata_s3_bucket,
        "added_keys": added_urls,
        "updated_keys": updated_urls,
        "deleted_keys": deleted_urls,
        "source": args.source_url,
        "target": f"/user-datasets/{args.workspace_name}/{args.sub_catalog}/",
    }

    if any([added_urls, updated_urls, deleted_urls]):
        # Send Pulsar message containing harvested links
        producer.send((json.dumps(output_data)).encode("utf-8"))
        logging.info(f"Sent harvested message {output_data}")
    else:
        logging.info("No changes made to previously harvested state")

    # Record harvested hash data in S3
    upload_file_s3(
        json.dumps(latest_harvested), args.metadata_s3_bucket, metadata_s3_key
    )
    logging.info(f"Harvested files: {latest_harvested}")
    logging.info(output_data)

    return output_data


if __name__ == "__main__":
    main()
