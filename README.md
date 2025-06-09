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

### Install via makefile

```commandline
make setup
```

This will create a virtual environment called `venv`, build `requirements.txt` and
`requirements-dev.txt` from `pyproject.toml` if they're out of date, install the Python
and Node dependencies and install `pre-commit`.

It's safe and fast to run `make setup` repeatedly as it will only update these things if
they have changed.

After `make setup` you can run `pre-commit` to run pre-commit checks on staged changes and
`pre-commit run --all-files` to run them on all files. This replicates the linter checks that
run from GitHub actions.


### Alternative installation

1. Follow the instructions [here](https://github.com/UKEODHP/template-python/blob/main/README.md) for installing a 
Python 3.11 environment.
2. Install dependencies:

```commandline
pip3 install -r requirements.txt
```
3. Set up environment variables
```commandline
export PULSAR_URL=<pulsar_url>
export AWS_ACCESS_KEY=<aws_access_key>
export AWS_SECRET_ACCESS_KEY=<aws_secret_access_key>
```
4. Run the service
```commandline
python -m stac_harvester <output_url>
```


## Configuration

STAC Harvesters are configured through environment variables


### Environment Variables

- `MINIMUM_MESSAGE_ENTRIES`: Minimum number of STAC entries to send per message 
- `TOPIC`: Topic suffix for pulsar messages

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
./venv/bin/python -m __main__.py https://example_source_url.com workspace_name source_folder target_folder s3_bucket
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
- `make requirements`: Update requirements files from `pyproject.toml`.
- `make requirements-update`: Update to the latest allowed versions.
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