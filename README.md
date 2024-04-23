# stac-harvester
A command line tool to harvest upstream STAC catalogs. This has the following responsibilities:
  - Command line tool to find STAC records from a given source catalog
  - Send Pulsar messages containing HTTP URLs of entries in the source catalog
  - Messages point to added, changed and deleted records only by comparison with stored hashes in S3
  - Dockerfile to build module for deployment


## Installation

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