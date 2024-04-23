# Changelog

## v0.1.0 (23-04-2024)

- Initial commit of STAC Harvester
  - Command line tool to find STAC records from a given source catalog
  - Send Pulsar messages containing HTTP URLs of entries in the source catalog
  - Messages point to added, changed and deleted records only by comparison with stored hashes in S3
  - Dockerfile to build module for deployment
