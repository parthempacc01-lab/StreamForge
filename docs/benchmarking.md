# Benchmarking EventStream

The local benchmark measures **`BrokerService::produce` batch latency**. It
includes record construction/encoding, file append and C++ stream flush. It
excludes TCP serialization/transfer, server scheduling, client wait time, CPU
and memory measurement. Separate single-client and concurrent TCP benchmarks
measure end-to-end request latency as defined below.

Each run uses the requested number of messages and records one sample per
`produce` batch. Ten warm-up batches are excluded by default; five measured
repetitions are run by default. p50/p95/p99 use sorted samples and the index
`floor(p * (N - 1))`. The aggregate throughput is the median of per-run
throughput; no failed operation is counted.

`appended` is the available Windows policy: the C++ stream is flushed. POSIX
supports a separate `synced` path, but the Windows benchmark rejects it rather
than reporting a fictional comparison.

## Reproduce

```powershell
cmake -S . -B build-release -G "Visual Studio 17 2022" -A x64
cmake --build build-release --config Release
ctest --test-dir build-release -C Release --output-on-failure
.\build-release\Release\eventstream_benchmark.exe .\bench-smoke --messages 1000 --payload-bytes 16 --batch-size 1
.\build-release\Release\eventstream_benchmark.exe .\bench-representative --messages 10000 --payload-bytes 256 --batch-size 16
```

Use a new empty data directory for every command. To cover the supported local
matrix manually, vary `--messages` over 10000/100000/1000000,
`--payload-bytes` over 16/256/1024/4096, and `--batch-size` over 1/16/64/256.
Do not run the entire matrix automatically: it can write many gigabytes.

The executable prints compiler/build/OS. CPU, memory and storage are currently
reported as `not_measured`; collect those with platform tools beside any report.
Local results are not a fair Kafka comparison without an equivalent controlled
workload, persistence policy, machine and measurement method.

## Recorded local results

These runs were executed on 2026-10-13 with `MSVC-19.44.35229.0`, Release,
and Windows. The host denied access to CPU model, memory capacity, and volume
metadata, so those fields are intentionally recorded as `not measured`.

| Workload | Median throughput | Median MiB/s | p50/p95/p99 batch latency |
| --- | ---: | ---: | ---: |
| 1,000 messages, 16 B, batch 1 | 2,194.32 msg/s | 0.03348 | 0.4249 / 0.8778 / 1.2035 ms |
| 10,000 messages, 256 B, batch 16 | 22,219.8 msg/s | 5.42474 | 0.7053 / 1.2887 / 1.8387 ms |

Both used five repetitions and ten warm-up batches. They are local append
results only, not TCP, concurrency, CPU, memory, or disk-flush measurements.

## TCP produce benchmark

`eventstream_tcp_benchmark` uses the same `PRODUCE_BATCH` frame, decoder, TCP
server, broker service, and response framing as the integration tests. It
starts an in-process loopback server for each isolated repetition, creates the
topic and connects before timing, and sends one request at a time. A latency
sample starts immediately before `send` and ends after the complete response
has been decoded and validated. Server start, topic creation, and connection
setup are excluded; request encoding and response parsing are included.

```powershell
.\build-release\Release\eventstream_tcp_benchmark.exe .\tcp-smoke --messages 1000 --payload-bytes 16 --batch-size 1
.\build-release\Release\eventstream_tcp_benchmark.exe .\tcp-representative --messages 10000 --payload-bytes 256 --batch-size 16
```

The supplied directory must not already contain `tcp-run-*` directories. The
benchmark supports `--clients 1`, `2`, `4`, `8`, or `16` and `--partitions 1|4`;
each client owns one TCP socket, frame decoder, and at most one outstanding
request. The requested `--messages` is the total measured message count and is
divided across clients. Client `i` always targets partition `i % partitions`:
`--partitions 1` is the shared-partition workload, while `--partitions 4`
spreads clients deterministically over four independent partitions. Every run
prints this mapping plus expected, client-successful, and broker-stored message
counts for each partition, and fails if any set differs.
Each repetition has isolated storage and a fresh in-process server. The server
is configured for the requested clients plus one temporary topic-creation
connection.

```powershell
.\build-release\Release\eventstream_tcp_benchmark.exe .\tcp-concurrent-smoke --messages 160 --payload-bytes 16 --batch-size 1 --clients 4 --warmup-batches 1 --repetitions 1
.\build-release\Release\eventstream_tcp_benchmark.exe .\tcp-concurrent-representative --messages 1000 --payload-bytes 256 --batch-size 16 --clients 4 --warmup-batches 2 --repetitions 5
.\build-release\Release\eventstream_tcp_benchmark.exe .\tcp-shared-4 --messages 1000 --payload-bytes 256 --batch-size 16 --clients 4 --partitions 1 --warmup-batches 2 --repetitions 5 --profile off
.\build-release\Release\eventstream_tcp_benchmark.exe .\tcp-independent-4 --messages 1000 --payload-bytes 256 --batch-size 16 --clients 4 --partitions 4 --warmup-batches 2 --repetitions 5 --profile off
```

Server startup, topic creation, connection setup, and warm-up requests are
excluded. Timing begins after all client workers reach a barrier and ends after
all measured workers finish. Each latency sample begins immediately before
`send` and ends after the complete response is decoded and validated. Request
encoding, TCP transfer, server queueing, broker work, response encoding, and
response parsing are included. p50/p95/p99 pool every measured batch sample
from every client and repetition, sort them, and select
`floor(p * (N - 1))`; they are not averages of per-client percentiles.

A failed send, invalid response, timeout, worker failure, or mismatch between
successful and requested messages (including per-partition totals) fails the
benchmark run. `--profile off` is the comparable measurement mode;
`--profile on` is a separate diagnostic mode that reports aggregate TCP
processing plus broker partition-lock wait/hold and append/flush time. This is
a loopback end-to-end measure, not a remote-network or power-loss durability
measurement. Multiple clients targeting different partitions can enter the
broker concurrently, but this does not prove linear scaling or production
readiness.

## Recorded concurrent TCP results

The following Release loopback measurements ran locally on 2026-10-13 on the
same Windows/MSVC environment noted above. They are small reproducibility
checks, not capacity claims. All runs reported zero failed requests.

| Clients | Workload | Successful messages | Median throughput | p50 / p95 / p99 request latency |
| ---: | --- | ---: | ---: | --- |
| 1 | 160 messages, 16 B, batch 1, 1 repetition | 160 | 1,051.29 msg/s | 1.0263 / 1.4405 / 1.5255 ms |
| 2 | 160 messages, 16 B, batch 1, 1 repetition | 160 | 1,139.99 msg/s | 1.7427 / 2.3514 / 3.0545 ms |
| 4 | 160 messages, 16 B, batch 1, 1 repetition | 160 | 1,349.99 msg/s | 2.7485 / 4.2551 / 5.1594 ms |
| 8 | 160 messages, 16 B, batch 1, 1 repetition | 160 | 1,866.98 msg/s | 3.5751 / 6.4725 / 6.8334 ms |
| 16 | 160 messages, 16 B, batch 1, 1 repetition | 160 | 1,763.65 msg/s | 8.2297 / 9.8880 / 10.0784 ms |
| 4 | 1,000 messages/run, 256 B, batch 16, 5 repetitions | 5,000 | 18,226.2 msg/s | 2.4446 / 5.1712 / 5.7235 ms |

The five-run 4-client workload had per-run throughput of 11,055.6, 14,066.5,
31,500.0, 18,226.2, and 28,477.5 messages/s. It predates the per-partition
benchmark layout and must not be used to compare shared versus independent
partitions. Do not infer that more clients will reliably increase throughput.

## Per-partition comparison results

The following Release loopback runs were executed locally on 2026-10-14 using
Windows/MSVC, 1,000 messages per repetition, 256 B payloads, two warm-up
batches, `appended` persistence, and five repetitions per row. They used
`--profile off`; the throughput range is the minimum–maximum of the five
per-run rates. Every run reported zero failed requests and exact expected and
successful per-partition message counts; later final verification runs also
confirmed stored counts through broker fetches. These are local observations, not
capacity claims or a Kafka comparison.

| Partitions | Clients | Batch | Median msg/s | Run range msg/s | p50 / p95 / p99 ms |
| ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 1 | 1 | 1,753.67 | 1,351.20–1,912.16 | 0.5176 / 1.1522 / 1.6083 |
| 1 | 4 | 1 | 1,785.67 | 1,649.18–2,051.73 | 2.0357 / 3.4894 / 4.0571 |
| 1 | 8 | 1 | 1,442.13 | 1,255.08–1,927.98 | 5.4007 / 7.7978 / 9.0046 |
| 1 | 16 | 1 | 1,551.32 | 1,339.88–1,607.51 | 10.6407 / 13.9080 / 15.9239 |
| 4 | 1 | 1 | 1,332.82 | 1,090.19–1,673.37 | 0.6959 / 1.3859 / 1.9185 |
| 4 | 4 | 1 | 4,191.95 | 3,824.06–4,329.50 | 0.7725 / 1.6368 / 2.5698 |
| 4 | 8 | 1 | 6,131.55 | 5,933.58–6,325.63 | 1.1713 / 1.7670 / 2.2722 |
| 4 | 16 | 1 | 6,497.16 | 5,859.71–6,697.93 | 2.3223 / 3.3512 / 4.5719 |

| Partitions | Clients | Batch | Median msg/s | Run range msg/s | p50 / p95 / p99 ms |
| ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 1 | 16 | 17,979.70 | 12,119.00–27,458.50 | 0.7515 / 1.6881 / 2.1653 |
| 1 | 4 | 16 | 17,442.40 | 16,150.40–21,278.00 | 3.2901 / 4.9655 / 5.9791 |
| 1 | 8 | 16 | 18,216.20 | 16,854.00–19,307.10 | 6.6325 / 8.6203 / 9.9636 |
| 1 | 16 | 16 | 18,703.50 | 17,156.20–21,391.40 | 12.7262 / 16.1767 / 18.3237 |
| 4 | 1 | 16 | 16,055.70 | 13,791.40–16,954.90 | 0.9376 / 1.6641 / 2.1884 |
| 4 | 4 | 16 | 48,007.00 | 40,912.00–54,219.30 | 1.0789 / 2.1870 / 2.7428 |
| 4 | 8 | 16 | 64,203.40 | 35,576.00–70,468.20 | 1.6176 / 4.1129 / 12.9516 |
| 4 | 16 | 16 | 73,517.50 | 66,435.00–84,068.90 | 2.9442 / 4.1818 / 4.6834 |

With four or more clients, the independent-partition layout outperformed the
shared partition in this workload. The separate profiling-on diagnostic runs
also showed far lower aggregate partition-lock wait with four independent
partitions (for example, 4 clients/batch 16: 0.0204–0.0257 ms per run versus
105.658–218.143 ms shared). Instrumentation changes timing, so these profiles
are explanatory only and are not compared against the profiling-off table.

## Linux ThreadSanitizer

Use the existing separate Linux TSan configuration; it must not be combined
with AddressSanitizer/UndefinedBehaviorSanitizer:

```bash
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DSTREAMFORGE_BUILD_TESTING=ON -DSTREAMFORGE_ENABLE_TSAN=ON
cmake --build build-tsan --parallel
TSAN_OPTIONS=halt_on_error=1 ctest --test-dir build-tsan --output-on-failure
```

A clean TSan run covers only the exercised workload; it does not prove race
freedom.
