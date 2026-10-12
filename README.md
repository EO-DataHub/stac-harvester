# stac-harvester
A command line tool to harvest upstream STAC catalogs. This has the following responsibilities:
  - Command line tool to find STAC records from a given source catalog
  - Harvests data to S3
  - Send Pulsar messages containing S3 paths of harvested entries in the source catalog
  - Messages point to added, changed and deleted records only by comparison with stored hashes in S3


## Features

- **Catalogue collection** - collects all entries in STAC catalogue
- **Check for updates** - checks upstream catalogue contents for changes and only processes these


## Getting Started

### Prerequisites

Install `uv` by following the instructions at:
https://docs.astral.sh/uv/getting-started/installation/

### Setup

Run the setup command to install dependencies and configure the development environment:

```commandline
make setup
```

This will install all dependencies and set up `pre-commit` hooks.

It's safe and fast to run `make setup` repeatedly as it will only update these things if
they have changed.

After `make setup` you can run `pre-commit` to run pre-commit checks on staged changes and
`pre-commit run --all-files` to run them on all files. This replicates the linter checks that
run from GitHub actions.


## Configuration

STAC Harvesters are configured through environment variables


### Environment Variables

- `MINIMUM_MESSAGE_ENTRIES`: Minimum number of STAC entries to send per message 
- `TOPIC`: Topic suffix for pulsar messages
- `PULSAR_URL`
- `AWS_ACCESS_KEY`
- `AWS_SECRET_ACCESS_KEY`
- `MAX_DELETE_FRACTION`: Largest fraction of the previously harvested keys that one run may delete (default `0.2`). Must be a number between 0 and 1, otherwise the run fails at startup. Only applies when more than 10 keys would be deleted
- `ALLOW_UNSAFE_DELETES`: Set to `true` on a one-off run to bypass the delete circuit breaker

### Delete safety

The harvester only sends `deleted_keys` when it is confident that the entries really are gone upstream:

- **Partial runs never delete.** If any listed entry is skipped or fails to fetch, if the fetched entry is not the
  listed one (different id or type), or if the listing fails or returns a malformed page, the run logs
  `HARVEST PARTIAL`, sends no deletes and merges the new hashes into the stored harvest metadata. A listed entry
  that permanently fails to fetch therefore suppresses all deletes for that harvester until it is fixed upstream;
  the `HARVEST PARTIAL` error on every run is expected in that case.
- **Collection harvesters only delete beneath their own collection.** When the source URL is a collection, the
  root catalogue and any other key outside that collection are never deleted. If the collection is no longer listed
  upstream the run fails without sending or storing anything.
- **Delete circuit breaker.** On a complete run no deletes are sent, and `DELETE CIRCUIT BREAKER` is logged, if
  they would include a catalogue or collection, if the listing returned no items although items were previously
  harvested, or if more than 10 keys and more than `MAX_DELETE_FRACTION` of the previously harvested keys would
  be deleted. Git harvests are exempt. Withheld keys stay in the harvest metadata and are reconsidered on every
  run: deletes resume on their own if the rules stop firing (for example because the missing entries are listed
  again), otherwise a one-off run with `ALLOW_UNSAFE_DELETES=true` is needed. The override bypasses the breaker
  only, never the two rules above.
- **Publish failures fail the run.** If harvested data cannot be stored or published, no deletes are sent, the
  harvest metadata is left untouched so that everything is sent again by the next run, and the run exits with an
  error.

### Removing a collection

A collection harvester never deletes its own collection, and fails on every run once the collection is gone
upstream. To remove a collection deliberately:

1. Remove the harvester's configuration so that it no longer runs.
2. Delete the collection through the catalogue API, or publish a `harvested` message listing its keys in
   `deleted_keys`.
3. Delete the harvester's `harvested-metadata/` object from the metadata S3 bucket.

For a harvester of a whole catalogue, a one-off run with `ALLOW_UNSAFE_DELETES=true` sends the withheld deletes.

## Pulsar Messages
### Outgoing Pulsar Messages (`harvested` topic)

After processing, the service sends a message to the `harvested` topic. 

```json
{
  "id": "<unique_id>",
  "workspace": "<workspace_name>",
  "bucket_name": "<destination_s3_bucket>",
  "source": "<source_url_prefix>",
  "target": "<target_url_prefix>",
  "updated_keys": ["<list/of/transformed/keys>"],
  "deleted_keys": ["<list/of/deleted/keys>"],
  "added_keys": ["<list/of/newly/added/keys>"]
}
```

- `bucket_name`: S3 bucket where files are stored (may be different from the source).
- `updated_keys`, `deleted_keys`, `added_keys`: Refer to the updated, deleted, or added files in the output catalogue.

**Note:** The service may also send "empty" catalogue change messages (with no updated, deleted, or added keys) to indicate no file changes.

## Usage

The service is typically run as part of a data pipeline, but you can invoke it directly for testing or development.

Run the STAC Harvester from the command line:

```sh
uv run python -m __main__.py https://example_source_url.com workspace_name source_folder target_folder s3_bucket
```

## Development

- Code is in `stac_harvester`.


- Formatting: [Black](https://black.readthedocs.io/), [Ruff](https://docs.astral.sh/ruff/), [isort](https://pycqa.github.io/isort/).
- Linting: [Pylint](https://pylint.pycqa.org/).
- Pre-commit checks are installed with `make setup`.

Useful Makefile targets:

- `make setup`: Set up or update the dev environment.
- `make test`: Run tests continuously.
- `make testonce`: Run tests once.
- `make lint`: Run all linters and formatters.
- `make dockerbuild`: Build a Docker image.
- `make dockerpush`: Push a Docker image.


## Testing

Run all tests with:

```sh
make testonce
```

Tests use [pytest](https://docs.pytest.org/), [moto](https://github.com/spulec/moto) for AWS mocking, and [requests-mock](https://requests-mock.readthedocs.io/).

## Troubleshooting

- **Authentication errors:** Ensure your `AWS_ACCESS_KEY` and `AWS_SECRET_ACCESS_KEY` are set correctly and have permission to access the required S3 buckets.
- **Pulsar connection issues:** Check that `PULSAR_URL` is set to the correct broker address and is reachable from your environment.
- **S3 upload or download failures:** Verify that any relevant buckets exist, your credentials have the correct permissions, and the bucket region matches your configuration.

Check the application logs for detailed error messages.

## Release Process

The release process is fully automated and handled through GitHub Actions.  
On every push to `main` or when a new tag is created, the following checks and steps are run automatically:

- Pre-commit checks and linting
- Security scanning
- Unit tests
- Docker image build and push to the configured registry

Versioned releases are handled through the Releases page in github.

See [`.github/workflows/actions.yaml`](.github/workflows/actions.yaml) for details.

## License

This project is licensed under the United Kingdom Research and Innovation BSD Licence. See [LICENSE](LICENSE) for details.