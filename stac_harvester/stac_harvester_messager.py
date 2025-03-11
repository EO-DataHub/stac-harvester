import json
from typing import Sequence

from eodhp_utils.messagers import Messager


class STACHarvesterMessager(Messager[str]):
    """
    Loads STAC Catalogue harvested files into an S3 bucket with file key relating to the
    owning catalog combined with the file path in the external catalogue.
    For example: stac-harvester/supported-datasets/test-catalog/collection/item
    Then sends a catalogue harvested message via Pulsar to trigger transformer and ingester.
    """

    def process_msg(self, msg: dict) -> Sequence[Messager.Action]:

        action_list = []
        harvested_data = msg["harvested_data"]
        deleted_keys = msg["deleted_keys"]

        for key, value in harvested_data.items():
            # Retrieve data
            stac_data = value
            # return action to save file to S3
            # bucket defaults to self.output_bucket

            action = Messager.OutputFileAction(
                file_body=json.dumps(stac_data),
                cat_path=key,
            )
            action_list.append(action)

        for key in deleted_keys:
            # return action to delete file from S3
            action = Messager.OutputFileAction(file_body=None, cat_path=key)
            action_list.append(action)

        return action_list

    def gen_empty_catalogue_message(self, msg):
        return {
            "id": f"stac-harvester/{msg['source']}",
            "workspace": "default_workspace",
            "repository": "",
            "branch": "",
            "bucket_name": self.output_bucket,
            "source": msg["source"],
            "target": msg["target"],
        }
