# StreamForge protocol v1

This is StreamForge's custom protocol. It is not compatible with Kafka.
All six wire operations are implemented end-to-end: `PING`, `CREATE_TOPIC`,
`PRODUCE_BATCH`, `FETCH`, `COMMIT_OFFSETS`, and `GET_METRICS`. The remaining
reserved codes (`DESCRIBE_TOPIC`, `JOIN_GROUP`, `HEARTBEAT`, `LEAVE_GROUP`)
return a stable `UnknownOperation` response.

## Frame layout

All unsigned integers are big-endian. A TCP connection is a byte stream: a
read may contain part of a frame, exactly one frame, or many frames.

| Field | Size | Meaning |
| --- | ---: | --- |
| `frame_length` | 4 bytes | Body bytes after this field; 16 through configured maximum |
| `magic` | 2 bytes | `0x5346` (`SF`) |
| `version` | 1 byte | Protocol version; currently `1` |
| `operation` | 1 byte | Request operation code |
| `flags` | 2 bytes | Reserved; must be interpreted by the operation schema later |
| `request_id` | 8 bytes | Client-chosen response correlation identifier |
| `status` | 2 bytes | `0` for a request; response/error status later |
| `payload` | variable | Operation-specific bytes; not yet defined |

The default maximum body length is 4 MiB. The codec rejects an undersized or
oversized length immediately after receiving the four-byte prefix, before
waiting for or allocating a body. It then rejects invalid magic, unsupported
versions, and unknown operations. A decoder error is terminal for that
connection; its owner must close the connection rather than resynchronizing an
untrusted byte stream.

## Defined operation codes

`PING=1`, `CREATE_TOPIC=2`, `DESCRIBE_TOPIC=3`, `PRODUCE_BATCH=4`,
`FETCH=5`, `COMMIT_OFFSETS=6`, `JOIN_GROUP=7`, `HEARTBEAT=8`,
`LEAVE_GROUP=9`, and `GET_METRICS=10`.

`PING` echoes its payload in an `OK` response. `CREATE_TOPIC` has a request
payload of `name_length:u16`, exactly `name_length` UTF-8 bytes, then
`partition_count:u32`. It returns an empty `OK` payload after the synchronous
broker call succeeds. Both integers are big-endian. A malformed create request
returns `MalformedFrame`; an unimplemented but reserved operation returns
`UnknownOperation`. Responses copy the request ID so clients can match them.

`PRODUCE_BATCH` is `name_length:u16`, topic bytes, `partition:u32`,
`ack_mode:u8` (currently `0`, appended), `record_count:u32`, then records of
`timestamp_ms:i64`, `key_length:u32`, key bytes, `value_length:u32`, value
bytes. The response is `first_offset:u64`, `last_offset:u64`.

`FETCH` is `name_length:u16`, topic bytes, `partition:u32`, `start_offset:u64`,
and `maximum_records:u32` (at most 1000). Its response is `next_offset:u64`,
`record_count:u32`, then records containing `offset:u64`, `timestamp_ms:i64`,
key/value lengths and their bytes. All integer fields remain big-endian.

`COMMIT_OFFSETS` is `group_length:u16`, group bytes, `topic_length:u16`, topic
bytes, `partition:u32`, and `next_offset:u64`. An empty `OK` response confirms
the offset was durably recorded.

## Stable framing errors

- `invalid_length`: body shorter than the fixed 16-byte header.
- `oversized_frame`: declared body exceeds the decoder configuration.
- `invalid_magic`: bytes are not a StreamForge frame.
- `unsupported_version`: no compatible decoder is available.
- `unknown_operation`: the header operation byte is not reserved by v1.

The TCP layer will map these internal errors to stable protocol responses only
when enough trusted header data is available; otherwise it closes the peer.
