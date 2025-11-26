# Changelog

## v0.10 (26-11-2025)

### Fixed

- Skip non-fetchable items

## v0.1.9 (26-03-2025)

- Add top level harvest for collections

## v0.1.8 (07-03-2025)

- Change path format
- Send paths to transformer instead of files

## v0.1.7 (10-02-2025)

- Topic can be updated with environment variables

## v0.1.6 (16-12-2024)

- Refactor actions
- Refactor to use eodhp-utils

## v0.1.5 (11-10-2024)

- Add chunking to pulsar messages

## v0.1.4 (09-07-2024)

- Support nested catalogs in file keys
  - Update target output to support nested catalogs within file keys
  - Updated input argument, "catalog", to define full catalog path, including workspace
  - Updated unit tests

## v0.1.3 (20-05-2024)

- Use Pystac

## v0.1.2 (20-05-2024)

- Add curl and kubectl for deployment


## v0.1.1 (08-05-2024)

- Add limit for number of items to return

## v0.1.0 (23-04-2024)

- Initial commit of STAC Harvester
  - Command line tool to find STAC records from a given source catalog
  - Send Pulsar messages containing HTTP URLs of entries in the source catalog
  - Messages point to added, changed and deleted records only by comparison with stored hashes in S3
  - Dockerfile to build module for deployment
