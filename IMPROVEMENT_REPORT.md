# KV Store Improvement Report

## Executive Summary

The original quorum demonstration has been improved into a single-leader, in-memory KV prototype with clearer semantics, safer concurrency, fault injection, and reproducible measurements. The central fixes are atomic per-key version allocation, parallel replication/read fan-out, protection against older replicas overwriting newer values, and explicit timeout responses indicating an indeterminate write that has already been applied locally.

This is not Raft or Paxos. It does not provide persistence, automatic leader election, a write-ahead log, anti-entropy repair, or cross-leader conflict resolution. Its purpose is an explainable consistency/latency experiment, not a production database.

## Problems and Changes

| Original problem | Implementation | Verified outcome |
|---|---|---|
| Separate get/put version allocation could duplicate versions under concurrency | Atomic increment and installation using per-key `ConcurrentHashMap.compute` | Same-key concurrency test passes |
| Serial replication stopped sending after W acknowledgements | Parallel fan-out using a bounded executor; respond at W while submitted work continues | A slow peer does not block fast quorum; W=1 still sends to all peers |
| Out-of-order replication could overwrite newer values | Followers retain higher versions; conflicting values at the same version return 409 | Stale-overwrite and same-version conflict tests pass |
| Reads treated 404 as an exception rather than evidence of absence | Reachable 404 responses count toward R; select the highest collected version | Missing keys return 404 after quorum; mixed-version reads select the newest |
| Slow peers could delay operations without clear failure semantics | Configurable connect/read/quorum timeouts; failed W returns 503 with `status=indeterminate` and `localApplied=true` | Timeout/partial-write test passes |
| Configuration lacked range validation | Validate W/R, timeouts, delays, and thread counts; trim/deduplicate peer URLs | Invalid configuration fails early |
| Limited observability | `/health` reports role, failed replications, queued tasks, and active tasks | Supports demonstrations and fault diagnosis |

## Operation Semantics

- Client writes use `PUT /kv` on the leader. The leader atomically updates its local value, then submits replication to all configured peers.
- A 201 response requires W acknowledgements including the leader. Already-submitted peer tasks continue after the response; there is no durable retry queue.
- Failure to reach W before `QUORUM_TIMEOUT_MS` returns 503. Local state may already have changed and peers may complete later, so clients must not interpret this as proof that nothing was written. The response explicitly marks the outcome `indeterminate`.
- `GET /kv/{key}` collects R reachable responses and returns the highest version among them. If quorum is reached and all responses report absence, it returns 404; if R cannot be reached, it returns 503.
- W+R>N helps obtain current values only under assumptions including a single leader, monotonic versions, and a stable node set. Without a leader-election/consensus protocol, this prototype does not guarantee production-grade linearizability under network partitions.

## Automated Tests

The final Maven run on September 13, 2026 passed all 10 tests with zero failures and zero errors.

| Suite | Tests | Coverage |
|---|---:|---|
| `KvCorrectnessTests` | 3 | Out-of-order replication, read quorum, concurrent same-key versions |
| `KvReplicationTests` | 6 | Slow peers, W=1 fan-out, timeout/partial writes, absent keys, newest-version reads, conflicts and invalid inputs |
| `NodeApplicationTests` | 1 | Spring context startup |

The original implementation failed all three core correctness tests. This comparison demonstrates fixes for reproducible defects rather than merely increasing test count.

## Benchmark Method

`benchmark.py` uses the Python standard library to start five real JVM nodes. It uses deterministic seeds, 20 hot keys, 16 client threads, 200 warm-up requests, and 600 measured requests per run. Each standard-matrix case runs three times, with medians reported below. The full local run saves per-request records, run-level CSVs, aggregate JSON, environment metadata, and JAR/script SHA-256 hashes.

| Scenario (median) | TPS | Success rate | Mean | P95 | P99 | Stale-read rate |
|---|---:|---:|---:|---:|---:|---:|
| W3/R3, 50/50, no delay | 1,145.4 | 100% | 13.2ms | 36.3ms | 47.8ms | 0% |
| W1/R1, 50/50, no delay | 1,549.0 | 100% | 10.0ms | 24.8ms | 38.0ms | 0.67% (median) |
| W5/R1, 50/50, no delay | 1,076.3 | 100% | 14.6ms | 31.6ms | 38.2ms | 0% |
| W1/R5, 50/50, no delay | 990.2 | 100% | 15.7ms | 32.4ms | 46.0ms | 0% |
| W3/R3, 50ms replication delay | 355.6 | 99.83% | 43.9ms | 87.2ms | 95.4ms | 0% |
| W3/R3, 200ms replication delay | 144.4 | 100% | 108.0ms | 230.4ms | 237.5ms | 0% |
| W3/R3, one slow peer | 123.3 | 100% | 124.0ms | 375.9ms | 469.8ms | 0% |
| W3/R3, one unreachable peer | 1,249.7 | 100% | 12.2ms | 25.4ms | 34.1ms | 0% |

W1/R1 permits replication lag to be visible through local reads, producing a nonzero stale-read rate. No stale reads were observed in the higher-quorum cases in this finite matrix. An observed 0% is not a theoretical guarantee.

In the matched high-delay comparison—200ms replication delay, 200ms leader write delay, and 50ms read delay—the original JAR achieves median TPS of 44.1 versus 64.5 for the improved JAR, approximately 46.2% higher. Mean latency falls from 353.7ms to 244.1ms and P95 from 649.7ms to 433.3ms. Local JVM scheduling affects this comparison; it is demonstration evidence, not a cross-machine capacity commitment.

Some high-concurrency repetitions have transient request failures. All success rates are retained in the aggregate evidence; no anomalous repetitions were removed to improve results. A next-stage experiment should add controlled arrival rates, client failure/retry classification, and server executor metrics.

## Reproduction and Evidence

```powershell
cd node
mvn test
mvn package
cd ..
python benchmark.py --requests 600 --warmup 200 --output evidence/benchmark
```

- Matrix aggregates: `evidence/benchmark-final/summary.json`
- Run-level measurements: `evidence/benchmark-final/results.csv`
- Environment and hashes: `evidence/benchmark-final/environment.json`
- Original-implementation comparison: `evidence/baseline-benchmark/`
- Automated tests: `node/src/test/java/com/kv/node/`

The GitHub repository includes compact summaries and environment metadata. Per-request records and node logs remain in the full local project and are excluded from the upload. Running the benchmark regenerates these detailed outputs.

## Remaining Boundaries

Production-oriented extensions would prioritize durable logging and crash recovery, idempotency keys, background retry/anti-entropy, membership changes and health-based exclusion, leader epochs/consensus, authentication, and rate limiting. This implementation stops at the improvement plan's scope: atomic versions, parallel quorum, explicit timeout semantics, fault tests, and a reproducible benchmark matrix.
