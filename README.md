# StreamForge (EventStream)

[![CI](https://github.com/parthempacc01-lab/StreamForge/actions/workflows/ci.yml/badge.svg)](https://github.com/parthempacc01-lab/StreamForge/actions/workflows/ci.yml)
[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![CMake](https://img.shields.io/badge/CMake-3.25%2B-064F8C.svg)](https://cmake.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A high-performance, append-only partitioned commit log and message broker built from scratch in modern C++20. StreamForge demonstrates production systems engineering concepts: custom binary wire framing, persistent segment-log storage with companion index seeking, atomic crash recovery, size-based retention, multi-threaded client coordination, and Prometheus observability.

---

## Key Features

- **Custom Binary Framing Protocol**: Custom binary framing with 32-bit CRC verification, 64-bit request IDs, and streaming bounded incremental decoding.
- **Persistent Log Storage Engine (`SegmentLog`)**:
  - Segment rolling driven by configurable byte thresholds (`maximum_segment_bytes`).
  - Companion binary offset index files (`.index`) with binary-search-based lookup (`std::lower_bound`) to seek directly to the physical file position and read only the requested bounded byte slice, eliminating full segment reads into memory.
  - Size-based retention policy (`retention_bytes`) with automatic tail and base-offset pruning.
  - Tail-repair crash recovery that auto-truncates unaligned/torn writes while rejecting corrupted records.
- **Concurrency & Multithreading**:
  - Fine-grained locking hierarchy: shared lifecycle and catalog locks with per-partition serialized mutexes for concurrent produce/fetch across topics.
- **Consumer Group Coordination & At-Least-Once Delivery**:
  - Durable committed offsets and topic metadata with atomic temporary-file replacement and explicit hardware synchronization (`fsync` of temp file and parent directory on POSIX; `FlushFileBuffers` on Windows).
  - Experimental Group Coordinator: in-memory partition assignment/rebalancing prototype; group-management operations are not yet exposed through the TCP protocol.
- **Production Observability**:
  - Native Prometheus exposition format (`GET_METRICS`) reporting live $p50$, $p95$, and $p99$ latency percentiles, record counts, byte throughput, and active connections.
- **Sanitizer & CI Verified**:
  - Automated GitHub Actions CI pipeline running AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), and ThreadSanitizer (TSan) on Linux and MSVC on Windows.

---

## Architecture Overview

See [docs/architecture.md](docs/architecture.md) for detailed concurrency locking designs, thread safety models, and component flowcharts.

```text
client ── TCP Frames (Protocol V1) ──> TcpServer ──> BrokerService
                                           │              ├───────> TopicCatalog (Atomic Metadata)
                                           │              ├───────> SegmentLog (.log + .index)
                                           │              ├───────> OffsetStore (Durable Offsets)
                                           │              └───────> GroupCoordinator (Rebalance)
                                           └── MetricsRegistry (Prometheus p50/p95/p99)
```

---

## Quickstart & Demo

### Prerequisites
- CMake 3.25+
- C++20 compliant compiler (MSVC 2022, GCC 11+, or Clang 14+)

### Build & Run (Windows PowerShell)

```powershell
# 1. Configure and Build
cmake -S . -B build-msvc -G "Visual Studio 17 2022" -A x64
cmake --build build-msvc --config Release

# 2. Run Test Suite (All 56 Unit & Integration Tests)
ctest --test-dir build-msvc -C Release --output-on-failure

# 3. Start Broker Daemon
.\build-msvc\Release\eventstream_server.exe .\data 9092

# 4. In another terminal, run sample client:
.\build-msvc\Release\eventstream_client.exe 127.0.0.1 9092 ping
```

### Build & Run with Sanitizers (Linux)

```bash
# AddressSanitizer & UndefinedBehaviorSanitizer
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DSTREAMFORGE_ENABLE_SANITIZERS=ON
cmake --build build-asan --parallel
ctest --test-dir build-asan --output-on-failure

# ThreadSanitizer (TSan) Concurrency & Race Verification
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DSTREAMFORGE_ENABLE_TSAN=ON
cmake --build build-tsan --parallel
ctest --test-dir build-tsan --output-on-failure
```


## Concurrency checks

The TCP integration tests include multiple independent clients producing at the
same time. They verify per-partition contiguous offsets and that every uniquely
identified successful record can be fetched; they do not establish parallel
broker execution, network fault tolerance, or race freedom.

On Linux, ThreadSanitizer is available as a separate Debug build (it must not
be combined with the AddressSanitizer/UndefinedBehaviorSanitizer option):

```bash
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DSTREAMFORGE_ENABLE_TSAN=ON
cmake --build build-tsan --parallel
ctest --test-dir build-tsan --output-on-failure -R "TcpServerTest.ConcurrentClients"
ctest --test-dir build-tsan --output-on-failure
```

ThreadSanitizer reports observed data races for the exercised run; a clean run
does not prove the server is race-free.

## Supported protocol operations

`PING`, `CREATE_TOPIC`, `PRODUCE_BATCH`, `FETCH`, `COMMIT_OFFSETS`, and `GET_METRICS` (Prometheus exposition format) are
implemented. All other reserved operations return a stable unsupported status.
Request and response formats are documented in `docs/protocol-v1.md`.


## Persistence, indexing, and retention

Records are append-only and checksummed:
- **Segment Rolling & Dense Indexing**: Segments roll automatically when reaching configured byte thresholds. Each `.log` segment file has a companion `.index` file recording every record's physical position. During `FETCH`, a `std::lower_bound` binary search on the index locates the starting physical file position and bounded byte range before seeking directly and reading only the required record bytes into memory.
- **Retention**: Size-based retention policies (`retention_bytes`) prune the oldest sealed segments when total log footprint exceeds limits.
- **Delivery & Recovery**: On startup, incomplete final records are repaired; corrupted records cause startup failure. Topic metadata and committed consumer offsets are persisted atomically through temporary-file replacement and explicit hardware persistence (write -> flush -> fsync temp -> rename -> fsync parent directory on POSIX, `FlushFileBuffers` on Windows). Committed offsets support **at-least-once** consumption when consumers commit strictly after processing. Synchronous segment durability (`AcknowledgementMode::kSynced`) flushes and synchronizes segment log files to non-volatile storage.

## Benchmark Summary

Performance is evaluated using dedicated, reproducible benchmark binaries with explicit workload parameters. Detailed methodology, hardware limits, lock contention diagnostics, and full client scaling matrices are documented in [docs/benchmarking.md](docs/benchmarking.md).

### Headline Workload: Concurrent TCP Produce (Release, 4 Partitions)

This headline scenario measures end-to-end multi-client TCP produce throughput and request latency across independent partitions:

```powershell
.\build-release\Release\eventstream_tcp_benchmark.exe .\tcp-independent-4 `
  --messages 1000 --payload-bytes 256 --batch-size 16 `
  --clients 4 --partitions 4 --warmup-batches 2 --repetitions 5 --profile off
```

- **Configuration**:
  - **Payload Size**: 256 bytes per record
  - **Batch Size**: 16 records per produce request
  - **Client Count**: 4 concurrent clients (each client on a dedicated TCP socket)
  - **Partition Count**: 4 independent partitions (deterministic assignment: client $i \to \text{partition } i \pmod 4$)
  - **Persistence Mode**: `appended` (C++ stream flushed to OS page cache; synchronous hardware `fsync` is unavailable on Windows)
  - **Repetitions & Volume**: 5 isolated runs × 1,000 messages = 5,000 total verified messages (2 warm-up batches discarded per run)
  - **Measurement Method**: End-to-end client latency, timed from immediately before socket `send()` through complete wire response decoding and validation. Excludes broker startup, topic creation, connection handshake, and warm-up. Throughput is the median across 5 runs; percentiles pool all measured batch samples using index $\lfloor p \cdot (N - 1) \rfloor$. Stored messages and offsets are verified via broker fetch after each run.

- **Headline Results (Windows x64, MSVC 19.44, Release)**:
  - **Median Throughput**: **48,007 msgs/s** (11.72 MiB/s) — run range: 40,912 to 54,219 msgs/s
  - **Latency Percentiles**: **p50 = 1.08 ms**, **p95 = 2.19 ms**, **p99 = 2.74 ms**
  - **Reliability**: 0 failed requests; 100% stored record and offset count integrity confirmed across partitions.

### Storage Engine Baseline: Local In-Process Append

To isolate underlying storage-engine throughput and batch latency from networking and socket scheduling overheads:

```powershell
.\build-release\Release\eventstream_benchmark.exe .\bench-representative `
  --messages 10000 --payload-bytes 256 --batch-size 16
```

- **Configuration**: 256-byte payload, batch size 16, 1 client, 1 partition, persistence mode `appended`, 5 runs × 10,000 messages.
- **Measurement Method**: Measures direct in-process `BrokerService::produce` batch execution (record serialization, segment log append, and stream flush; excludes TCP serialization, network transfer, and server event loop).
- **Baseline Results**:
  - **Median Throughput**: **22,220 msgs/s** (5.42 MiB/s)
  - **Latency Percentiles**: **p50 = 0.71 ms**, **p95 = 1.29 ms**, **p99 = 1.84 ms**

> [!NOTE]
> **Measurement Boundaries**: Local append and TCP produce evaluate different subsystem boundaries (in-process API batch append vs. multi-client loopback TCP network round-trip) and are not directly comparable experiments.
>
> **Environment & Hardware**: Benchmarks executed on Windows (MSVC 19.44, Release). Per project policy, unavailable host hardware information (CPU model, memory capacity, storage bus/media) is recorded honestly as `not measured` rather than assumed.

### Key Takeaways from the Full Benchmark Matrix

- **Shared vs. Independent Partition Contention**: Under the identical 4-client / 16-record batch workload, writing to 4 independent partitions achieves **48,007 msgs/s** (p50: 1.08 ms) compared to **17,442 msgs/s** (p50: 3.29 ms) on a single shared partition where requests serialize on the per-partition mutex.
- **Client Scaling**: Increasing concurrency to 16 clients across 4 independent partitions scales throughput to **73,518 msgs/s** (p50: 2.94 ms).
- **Comprehensive Matrix**: See [docs/benchmarking.md](docs/benchmarking.md) for complete results across 1–16 clients, 1 vs 4 partitions, single-record vs 16-record batches, and internal lock profiling (`--profile on`).

## Scope and limits

The group coordinator is currently an in-memory prototype, while consumer offsets and segment indexes are durable.
Replication is synchronous copying between in-process leader/follower brokers for testing, not cross-machine failover.
CI workflows are configured for Windows and Linux builds with AddressSanitizer and ThreadSanitizer; remote run status must be verified in GitHub Actions.
TLS, authentication, cross-process replication, and distributed Raft leader election are future work.



