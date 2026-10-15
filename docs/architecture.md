# StreamForge / EventStream Architecture

StreamForge is an append-only, partitioned commit log and message broker implemented in C++20. It models the core architectural principles of distributed stream logs (such as Apache Kafka and Apache Pulsar).

```text
               ┌────────────────────────────────────────────────────────┐
               │                      Client TCP                        │
               └──────────────────────────┬─────────────────────────────┘
                                          │  [Framed Protocol V1]
                                          ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ TcpServer (Accept Loop & Connection Threads)                                           │
│  - Incremental Bounded FrameDecoder (Max body limits, CRC, Magic byte)                 │
│  - Active connection tracking & graceful drain shutdown                                │
│  - Metrics collection (Prometheus Exposition format)                                   │
└─────────────────────────────────────────┬──────────────────────────────────────────────┘
                                          │
                                          ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ BrokerService (Façade & Concurrency Coordinator)                                       │
│  - Lifecycle Shared Mutex (Concurrent reads/writes vs exclusive startup recovery)      │
│  - Catalog Shared Mutex (Protected topic-partition mapping)                            │
│  - Per-Partition State & Mutex (Per-partition serialized write pipeline)               │
└────────────┬────────────────────────────┬─────────────────────────────┬────────────────┘
             │                            │                             │
             ▼                            ▼                             ▼
┌────────────────────────┐   ┌───────────────────────────┐   ┌───────────────────────────┐
│ TopicCatalog           │   │ OffsetStore               │   │ GroupCoordinator          │
│ - Durable metadata     │   │ - Committed consumer      │   │ - In-memory group         │
│ - Atomic temp-file     │   │   offsets per group       │   │   rebalance & partition   │
│   replacement          │   │ - Atomic fsync durability │   │   assignment              │
└────────────────────────┘   └───────────────────────────┘   └───────────────────────────┘
                                          │
                                          ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ Partition Storage Engine (SegmentLog)                                                  │
│                                                                                        │
│   segment-00000000000000000000.log      ◄── [Payload: Header + CRC32C + Key + Value]  │
│   segment-00000000000000000000.index    ◄── [Index Entry: Offset (8B) + Pos (8B)]      │
│                                                                                        │
│  - Active Segment: Append-only write stream with contiguous physical offset tracking   │
│  - Segment Rolling: Bounded segment size rolling (`maximum_segment_bytes`)             │
│  - Index Seeking: Binary search (`std::lower_bound`) on `.index` for physical position lookup │
│  - Retention: Size-based cleanup (`retention_bytes`) prunes oldest sealed segments     │
│  - Tail Repair: Auto-truncates incomplete torn frames on crash restart                 │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 1. Concurrency Model & Locking Hierarchy

To prevent deadlocks while maximizing concurrent throughput across independent topics and partitions, StreamForge enforces a strict lock hierarchy:

1. **`lifecycle_mutex_` (`std::shared_mutex`)**:
   - Acquired in **shared mode** by standard operational paths (`produce`, `fetch`, `commit_offset`).
   - Acquired in **exclusive mode** strictly during `recover()` on startup.
2. **`catalog_mutex_` (`std::shared_mutex`)**:
   - Protects the topic-to-partition map (`logs_`).
   - Released **before** interacting with the partition's underlying `SegmentLog` to avoid holding high-level locks during disk I/O.
3. **`PartitionState::mutex` (`std::mutex`)**:
   - Each partition has its own isolated mutex.
   - Operations on Partition 0 of Topic A never contend or block operations on Partition 1, enabling parallel broker execution across multi-core systems.
4. **`offset_mutex_` (`std::mutex`)**:
   - Serializes atomic writes to durable consumer offset files.

---

## 2. Storage & Durability Semantics

- **Append Pipeline**:
  - Each record contains a CRC-32C checksum, timestamp, key, and payload.
  - Appends are written sequentially to the active segment file and immediately indexed in the `.index` file.
  - Returns contiguous 64-bit offsets.
- **Index Lookups**:
  - The companion `.index` file holds compact 16-byte index entries (`[offset, position]`).
  - During `fetch`, the broker uses binary search (`std::lower_bound`) to seek directly to the nearest file position, preventing full table scans.
- **Delivery Guarantee**:
  - **At-least-once**: A record is considered committed once written to the process stream. If a client fails between processing and committing its offset, uncommitted records will be safely re-read.
- **Crash Recovery & Self-Healing**:
  - Startup scanning truncates incomplete or torn final records (e.g. from an ungraceful power cut).
  - Any corrupt record found prior to the tail fails broker initialization to prevent silent data loss.

---

## 3. Observability Architecture

- An atomic `MetricsRegistry` captures real-time broker vitals:
  - Total append/fetch operation counts, record throughput, and total payload bytes.
  - Active and cumulative TCP client connections.
  - Latency percentiles: $p50$, $p95$, and $p99$ calculated dynamically for appends and fetches.
- Metrics are queried using the `kGetMetrics` protocol operation, returning formatted text ready for ingestion by Prometheus or OpenTelemetry collectors.

