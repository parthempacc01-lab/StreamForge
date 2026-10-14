# StreamForge segment record format v1

This format defines the bytes of an individual record inside a segment file,
companion offset index files (`.index`), and log retention policies.

Segment logs use paired data and offset index files:
- `segment-00000000000000000000.log`: Sequential append-only record payload.
- `segment-00000000000000000000.index`: Binary offset lookup index composed of 16-byte pairs (`[uint64_t offset, uint64_t file_position]`).
- Retention: Size-based retention (`retention_bytes`) prunes the oldest sealed segments when total log disk usage exceeds thresholds.


All integer fields are unsigned big-endian except `timestamp_unix_ms`, which
is a two's-complement signed 64-bit integer encoded as its big-endian bit
pattern.

| Field | Size | Meaning |
| --- | ---: | --- |
| `body_length` | 4 bytes | Bytes after this field, 30 through configured maximum |
| `record_version` | 1 byte | `1` |
| `attributes` | 1 byte | Reserved; zero in v1 |
| `offset` | 8 bytes | Partition-local assigned offset |
| `timestamp_unix_ms` | 8 bytes | Broker-supplied timestamp placeholder |
| `key_length` | 4 bytes | Key bytes immediately following the header |
| `value_length` | 4 bytes | Value bytes immediately after the key |
| `key` | variable | Opaque bytes |
| `value` | variable | Opaque bytes |
| `crc32c` | 4 bytes | CRC-32C of all body bytes before this checksum |

The body is at least 30 bytes: the 26-byte header plus checksum. The default
maximum body size is 1 MiB.

## Recovery rule

The scanner accepts only contiguous offsets beginning with the caller-provided
segment base offset. `SegmentLog::recover()` truncates an incomplete final
length prefix or declared final body precisely to `valid_bytes`. Invalid sizes,
completed records with bad checksums, unsupported
versions, invalid key/value lengths, and offset gaps are corruption even when
they occur at the final record. They must fail broker startup rather than be
silently discarded.
