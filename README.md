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
  - Companion binary offset index files (`.index`) with binary-search-based lookup (`std::lower_bound`) to locate the physical position before reading and forward-scanning segment bytes in memory.
  - Size-based retention policy (`retention_bytes`) with automatic tail and base-offset pruning.
  - Tail-repair crash recovery that auto-truncates unaligned/torn writes while rejecting corrupted records.
- **Concurrency & Multithreading**:
  - Fine-grained locking hierarchy: shared lifecycle and catalog locks with per-partition serialized mutexes for concurrent produce/fetch across topics.
- **Consumer Group Coordination & At-Least-Once Delivery**:
  - Durable committed offsets with atomic file replacement.
  - In-memory group coordinator with automatic partition rebalancing.
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

# 2. Run Test Suite (All 53 Unit & Integration Tests)
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
- **Segment Rolling & Dense Indexing**: Segments roll automatically when reaching configured byte thresholds. Each `.log` segment file has a companion `.index` file recording every record's physical position. During `FETCH`, a `std::lower_bound` binary search on the index locates the starting physical file position before the segment bytes are read into memory and scanned forward.
- **Retention**: Size-based retention policies (`retention_bytes`) prune the oldest sealed segments when total log footprint exceeds limits.
- **Delivery & Recovery**: On startup, incomplete final records are repaired; corrupted records cause startup failure. Topic metadata and committed consumer offsets are persisted atomically through temporary-file replacement. Committed offsets support **at-least-once** consumption when consumers commit strictly after processing. `std::ofstream::flush()` commits writes to OS buffers, but POSIX `fsync` (`AcknowledgementMode::kSynced`) is required for true hardware power-loss durability.

## Benchmark

See the reproducible methodology and limits in [docs/benchmarking.md](docs/benchmarking.md).

Run local and TCP benchmarks across configurations:

```powershell
# Debug baseline:
.\build-msvc\Debug\eventstream_tcp_benchmark.exe .\tcp-concurrent-data --messages 20000 --payload-bytes 256 --batch-size 16 --clients 4 --partitions 4

# Release optimized:
.\build-msvc\Release\eventstream_benchmark.exe .\benchmark-data-release --messages 50000 --payload-bytes 256 --batch-size 32
.\build-msvc\Release\eventstream_tcp_benchmark.exe .\tcp-concurrent-data-release --messages 20000 --payload-bytes 256 --batch-size 16 --clients 4 --partitions 4
```

### Empirical Benchmark Measurements (Windows x64, MSVC 19.44)

Empirical performance measured across 5 consecutive runs per benchmark on identical hardware:

| Build Mode | Scenario | Concurrency | Batch Size | Messages / Run | Throughput (msgs/sec) | Throughput (MiB/s) | p50 Latency | p95 Latency | p99 Latency |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Debug** | Local Append | 1 client | 32 records | 50,000 | 14,741 msgs/s | 3.59 MiB/s | 1.91 ms | 3.26 ms | 4.04 ms |
| **Debug** | Concurrent TCP Produce | 4 clients, 4 partitions | 16 records | 20,000 | 14,267 msgs/s | 3.48 MiB/s | 3.98 ms | 6.65 ms | 10.71 ms |
| **Release** | **Local Append** | 1 client | 32 records | 50,000 | **29,990 msgs/s** | **7.32 MiB/s** | **0.95 ms** | **1.65 ms** | **2.46 ms** |
| **Release** | **Concurrent TCP Produce** | 4 clients, 4 partitions | 16 records | 20,000 | **28,909 msgs/s** | **7.05 MiB/s** | **1.62 ms** | **3.66 ms** | **7.72 ms** |

*(Throughput figures represent median rates across 5 consecutive pooled runs; latency represents pooled sample percentiles).*

## Scope and limits

The group coordinator is currently an in-memory prototype, while consumer offsets and segment indexes are durable.
Replication is synchronous copying between in-process leader/follower brokers for testing, not cross-machine failover.
CI workflows are configured for Windows and Linux builds with AddressSanitizer and ThreadSanitizer; remote run status must be verified in GitHub Actions.
TLS, authentication, cross-process replication, and distributed Raft leader election are future work.



