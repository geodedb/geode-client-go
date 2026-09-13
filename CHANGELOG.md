# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security

- Read-only connections: `?read_only=true` (aliases `readonly`, `read-only`)
  refuses mutating statements client-side with `ErrReadOnly` before any bytes
  reach the wire, and sets `ExecuteRequest.read_only` so the engine refuses
  them too with GQLSTATE `42501` regardless of role. Classification
  (`IsMutatingStatement`) fails closed: empty, unparseable and unrecognised
  statements, unknown `CALL` targets, and any `;`-separated program containing
  a write are all treated as mutating (SONNY-806).
- `?` placeholders are no longer inlined into the query text. Every argument
  now crosses the wire as a typed `proto.Param` (`$_gqN`), so no argument value
  can reach the GQL parser as source text (SONNY-806).

### Changed

- Query arguments are now bound as: `database/sql`'s `driver.Value` scalar set
  (`nil`, `string`, `[]byte`, `bool`, `int64`, `float64`); `time.Time`, encoded
  as the historical `fmt.Sprintf("%v", t)` string (round-trippable via
  `ScanTime`); other Go integer/float types and anything implementing
  `driver.Valuer`, narrowed by `database/sql`'s default converter (`int` ->
  `int64`, `float32` -> `float64`, etc.); and slices, arrays and maps, which
  keep their structural `list_val`/`map_val` encoding. Any other struct or
  type is rejected with an error instead of being silently formatted into a
  string.
- Parameter names beginning with `_gq` are reserved by the driver and rejected.
