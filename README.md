# KV-Store

A distributed key-value store built on **leader/follower replication with quorum reads and writes**, implemented in Spring Boot (Java 17). It's designed to study the consistency/latency trade-off: the write quorum (W) and read quorum (R) are configurable, and a bundled load tester lets you observe stale-read ratio, throughput, and latency.

## Architecture

- **A single node program** (`node/`) whose role is decided by the `ROLE` environment variable: **leader** or **follower**.
- **Writes** always go to the leader: version allocation and local installation are atomic per key. Replication fans out to all followers in parallel; the leader responds after **W** acknowledgements (including itself), while already-submitted replication continues.
- **Reads** can hit any node: with `R=1` the node reads locally; with `R>1` the coordinator queries peers in parallel, requires **R reachable responses**, and returns the value with the highest version.
- **Timeouts are explicit**: a write that misses quorum returns `503` with `status=indeterminate` and `localApplied=true`; a read that cannot reach R nodes returns `503`.
- **Followers are monotonic**: older versions cannot overwrite newer values, and a conflicting value at the same version is rejected with `409`.
- Storage is an in-process `ConcurrentHashMap` (no persistence — data is lost on restart).
- Artificial delays simulate a real system: followers `sleep 200ms` on replicate, the leader `sleep 200ms` after a write, and reads `sleep 50ms`.

`docker-compose.yml` starts **5 nodes** by default (1 leader + 4 followers) with `W=3` and `R=3`.

## HTTP API

| Method | Path | Description |
|--------|------|-------------|
| `PUT` | `/kv` | Client write, body: `{"key":"...","value":"..."}` (leader only) |
| `GET` | `/kv/{key}` | Quorum read: collect from R nodes and return the newest version |
| `GET` | `/local_read/{key}` | Read this node only (no read delay; used for testing) |
| `PUT` | `/replicate` | Internal endpoint: follower receives replication from the leader |
| `GET` | `/health` | Role, failed replication count, queued and active fan-out tasks |

A successful write returns `201 {"key":..., "version":...}`; if the write quorum isn't reached it returns `503`.

## Configuration (environment variables)

| Variable | Default | Description |
|----------|---------|-------------|
| `ROLE` | `leader` | `leader` or `follower` |
| `FOLLOWER_URLS` | empty | Other node URLs, comma-separated (leader uses these for replication and read fan-out) |
| `WRITE_QUORUM_SIZE` | `1` | Write quorum W |
| `READ_QUORUM_SIZE` | `1` | Read quorum R |
| `QUORUM_TIMEOUT_MS` | `1000` | End-to-end quorum deadline and peer HTTP timeout |
| `REPLICATION_DELAY_MS` | `200` | Artificial follower replication delay |
| `WRITE_DELAY_MS` | `200` | Artificial leader write delay |
| `READ_DELAY_MS` | `50` | Artificial quorum-read delay |
| `FANOUT_THREADS` | `64` | Bounded fan-out executor thread count |

## Running

### Docker Compose (recommended)

```bash
# Build the jar first (the Dockerfile copies target/*.jar)
cd node && ./mvnw clean package -DskipTests && cd ..

docker compose up --build
```

Port mapping after startup: leader `8080`, followers `8081`–`8084`.

```bash
# Write
curl -X PUT localhost:8080/kv -H 'Content-Type: application/json' \
  -d '{"key":"foo","value":"bar"}'

# Read
curl localhost:8080/kv/foo
```

### Local single node

```bash
cd node
./mvnw spring-boot:run
```

## Correctness tests and benchmark

```bash
cd node
./mvnw test
./mvnw package
cd ..
python benchmark.py --requests 600 --warmup 200 --output evidence/benchmark
```

The 10 tests cover concurrent same-key writes, stale/out-of-order replication, slow and unreachable peers, W=1 fan-out, timeout/partial-write semantics, absent-key quorum reads, newest-version selection, invalid inputs, and Spring startup.

`benchmark.py` starts five real JVMs and runs a deterministic, repeated matrix over W/R, read/write ratio, artificial delay, and slow/unreachable faults. It records TPS, successful TPS, mean/P95/P99 latency, success rate, stale-read rate, per-request evidence, node logs, environment metadata, and hashes. See `IMPROVEMENT_REPORT.md` for the measured results and limitations.

## Project layout

```
KV-Store/
├── docker-compose.yml          # 5-node cluster (W=3, R=3)
└── node/
    ├── Dockerfile
    └── src/main/java/com/kv/node/
        ├── NodeApplication.java  # Spring Boot entry point + RestClient bean
        ├── KvController.java      # read / write / replicate endpoints
        ├── Config.java           # environment-variable configuration
        ├── Dto.java              # request bodies
        ├── VersionedValue.java   # value with a version number
        └── LoadTester.java       # load-testing tool
```

The original `LoadTester.java` remains as a small manual client; `benchmark.py` is the reproducible comparison harness.
