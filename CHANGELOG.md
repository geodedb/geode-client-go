# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security

- `?` placeholders are no longer inlined into the query text. Every argument
  now crosses the wire as a typed `proto.Param` (`$_gqN`), so no argument value
  can reach the GQL parser as source text (SONNY-806).

### Changed

- Arguments outside `database/sql`'s `driver.Value` set (plus slices, arrays
  and maps, which keep their structural `list_val`/`map_val` encoding) are now
  rejected with an error instead of being silently formatted into a string.
- Parameter names beginning with `_gq` are reserved by the driver and rejected.
