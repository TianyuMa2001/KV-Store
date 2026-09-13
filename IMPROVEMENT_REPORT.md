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
| Limited observability | `/health` reports serving PID, role, effective W/R and timing configuration, failed replications, queued tasks, and active tasks | Enables process/configuration verification and fault diagnosis |

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

All four Python harness regression tests also pass: acknowledgement validation, unreachable-replica write rejection, valid quorum acceptance, and listening-port detection.

## Benchmark Correction and Current Evidence

The legacy benchmark is withdrawn. In case 10 (W=5 with one allegedly unreachable replica), repetition 0 recorded 298 writes returning 201. This contradicts the intended scenario, not necessarily the controller: the harness killed a launch process without proving the serving JVM/port had stopped. The Oracle javapath executable can be a launcher. The old run did not record server PID or effective quorum configuration, so the exact historical cause cannot be established.

Artifact provenance was also ambiguous. The recorded benchmark JAR had SHA-256 `a7ee3a23a6b51b4874532fd3f72eec915f6603e34e9250e163ef21f0ca506472`; a packaged JAR remaining in the original local project had SHA-256 `3d75b614df58317180f01a00dece0bb3eb44404b0dc45d427bff83b466d91bcc` and lacked the improved controller's `ReplicaRead` class. Archive hash differences alone can reflect packaging metadata, but these artifacts also had different controller bytecode.

The corrected harness:

- Builds current source with Maven package (including Java tests) by default.
- Requires an explicit expected SHA-256 when skipping a build or using a custom artifact.
- Resolves java.home/bin/java and checks that the serving PID equals the owned process PID.
- Verifies effective role, W/R, timeout, and replication delay through /health.
- Confirms the failed replica's process exit, closed listening port, and failed HTTP health request.
- Rejects insufficient/misconfigured 201 acknowledgements and any non-503 write in the verified W=5/unreachable case.
- Preserves complete response bodies and validates shutdown; refuses changed JAR/source/harness files.
- Writes provenance before measurement and uses command-scoped Git directory trust, avoiding a late metadata failure.

A full corrected workload ran 12 cases × 3 repetitions with 600 measured requests, 200 warm-up requests, 20 hot keys, and 16 threads. The decisive W=5/unreachable result was:

| Repetition | Measured writes | HTTP 201 | HTTP 503 |
|---|---:|---:|---:|
| 0 | 298 | 0 | 298 |
| 1 | 300 | 0 | 300 |
| 2 | 283 | 0 | 283 |
| Total | 881 | 0 | 881 |

All initialization/warm-up writes also passed the non-201 invariant. Mixed read/write success rate must not be interpreted as write success rate. No acknowledged writes exist in this case, so stale-read eligibility is zero and stale rate is undefined, not 0%.

The full run completed all workloads and shutdown checks, but final Git provenance collection failed because the elevated user did not trust the sandbox-owned checkout. Its status note explicitly records that limitation. The final harness fixes provenance collection before measurement and is separately checked in `evidence/benchmark-quorum-regression/`.

The final end-to-end regression completed successfully with `status=verified`: three repetitions, 120 measured requests and 20 warm-up requests per repetition, five hot keys, and 16 threads. Its 186 measured writes (63/63/60) all returned 503, with zero 201 responses. Recorded JAR SHA-256 is `8ae483e47468cedd772915ea61e60e49b498271e67e8da0e63a42e5c5e7a9e6c`; harness SHA-256 is `9fedeec6aa418d9b04e1c45c8ae20806f964c752d8a275584b5090fdeaa24c4f`. Both match the final local artifacts.

The previous 46.2% performance uplift and legacy performance table are withdrawn. A valid comparative uplift requires remeasuring both implementations with equivalent verified identity/configuration and workloads. Some corrected load repetitions still contain request failures; these are retained rather than removed and do not imply a quorum violation.

## Reproduction and Evidence

```powershell
cd node
mvn test
cd ..
python -m unittest -v test_benchmark
python benchmark.py --requests 600 --warmup 200 --output evidence/benchmark-new
python benchmark.py --cases 10 --output evidence/quorum-new
```

Output directories must be empty. Compact evidence and provenance are retained in the repository; detailed request records, startup/fault proofs, and node logs remain local under each case directory.

The recorded source commit is the checkout's base commit at measurement time, with local source changes identified by the recorded file hashes. These historical provenance values are not rewritten to the later publishing commit. `validation.json` publishes the decisive write counts and a compact fault-proof extract without uploading raw request logs.

- Corrected full-workload results: `evidence/benchmark-verified/results.csv`, `summary.json`, and `EVIDENCE_STATUS.md`
- Final harness regression: `evidence/benchmark-quorum-regression/`
- Withdrawn historical evidence: status notes in `evidence/benchmark-final/` and `evidence/baseline-benchmark/`
- Four harness regression tests: `test_benchmark.py`

## Remaining Boundaries

Production-oriented extensions would prioritize durable logging and crash recovery, idempotency keys, background retry/anti-entropy, membership changes and health-based exclusion, leader epochs/consensus, authentication, and rate limiting. This implementation stops at the improvement plan's scope: atomic versions, parallel quorum, explicit timeout semantics, fault tests, and a reproducible benchmark matrix.
