import json
from typing import Sequence

import requests
from eodhp_utils.messagers import Messager


class STACHarvesterMessager(Messager[str]):
    """
    Loads STAC Catalogue harvested files into an S3 bucket with file key relating to the
    owning catalog combined with the file path in the external catalogue.
    For example: stac-harvester/supported-datasets/test-catalog/collection/item
    Then sends a catalogue harvested message via Pulsar to trigger transformer and ingester.
    """

    def process_msg(self, msg: tuple[str, bool]) -> Sequence[Messager.Action]:

        updated = msg["updated"]
        url = msg["url"]

        # msg is a url to a STAC entity to be downloaded into the S3 bucket
        if updated:
            # True means the entity exists
            # Retrieve data
            stac_data = requests.get(url).json()
            # return action to save file to S3
            # bucket defaults to self.output_bucket
            action = Messager.OutputFileAction(
                file_body=json.dumps(stac_data),
                cat_path=f"supported-datasets/{msg["url"].replace(msg["source"], msg["target"])}",
            )
        else:
            # False means the entity no longer exists
            # return an action to delete file from S3, using blank file_body
            # bucket defaults to self.output_bucket
            action = Messager.OutputFileAction(
                file_body=None,
                cat_path=f"supported-datasets/{msg["url"].replace(msg["source"], msg["target"])}",
            )

        return [action]

    def gen_empty_catalogue_message(self, msg):
        return {
            "id": f"stac-harvester/{msg["source"]}",
            "workspace": "default_workspace",
            "repository": "",
            "branch": "",
            "bucket_name": self.output_bucket,
            "source": msg["source"],
            "target": msg["target"],
        }
