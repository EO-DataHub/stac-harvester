# Changelog

## v0.1.15 (12-10-2026)

### Fixed

- A source outage can no longer cause deletions: any skipped, failed, mismatched, misplaced or
  malformed entry, or malformed or incomplete listing, marks the run as partial, which suppresses
  all deletes
- Collection harvesters only delete keys beneath their own collection; the shared root catalogue
  and other out-of-scope keys are never deleted
- Fail the run when the configured collection is not found upstream instead of harvesting only
  the root catalogue
- Retry fetches with backoff and a timeout, including failures while reading the response; abort
  the harvest after repeated fetch failures or undecodable responses (entries answering 404 or
  410 do not count)
- Stop and fail the run, without sending deletes or updating the harvest metadata, when harvested
  data cannot be published; deletes are sent in a message of their own after all updates

### Added

- Delete circuit breaker: deletes are withheld if they include a catalogue or collection, if the
  listing returned no items although items were previously harvested, or if they exceed
  `MAX_DELETE_FRACTION` (default 0.2, must be between 0 and 1) of previously harvested keys.
  Withheld keys are reconsidered on every run; `ALLOW_UNSAFE_DELETES=true` overrides the breaker
  for a one-off run

### Changed

- After a partial run the harvest metadata is merged (new hashes recorded, unseen keys kept)
  rather than left untouched. A listed entry that permanently fails to fetch therefore suppresses
  all deletes for that harvester, and logs `HARVEST PARTIAL` on every run, until fixed upstream
- Handle malformed STAC item-search results safely

## v0.1.14 (23-03-2026)

### Fixed

- Set logging verbosity to INFO level (#34)

## v0.1.13 (18-03-2026)

### Fixed

- Retry harvester on request failures

## v0.1.12 (03-03-2025)

### Fixed

- Catch all per-item exceptions in harvest loop

## v0.1.11 (09-02-2025)

### Changed

- Use reusable QA GitHub Action

## v0.1.10 (26-11-2025)

### Fixed

- Skip non-fetchable items

### Changed

- Migrate Python package management to uv

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
