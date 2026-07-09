# KV-Store

A distributed key-value store built on **leader/follower replication with quorum reads and writes**, implemented in Spring Boot (Java 17). It's designed to study the consistency/latency trade-off: the write quorum (W) and read quorum (R) are configurable, and a bundled load tester lets you observe stale-read ratio, throughput, and latency.

## Architecture

- **A single node program** (`node/`) whose role is decided by the `ROLE` environment variable: **leader** or **follower**.
- **Writes** always go to the leader: the leader assigns a version number, writes locally first, then replicates to followers one by one until it has collected **W** acknowledgements (including its own), at which point it returns success.
- **Reads** can hit any node: with `R=1` the node reads locally only; with `R>1` the coordinating node reads itself and queries `R-1` followers, then returns the value with the **highest version**.
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

A successful write returns `201 {"key":..., "version":...}`; if the write quorum isn't reached it returns `503`.

## Configuration (environment variables)

| Variable | Default | Description |
|----------|---------|-------------|
| `ROLE` | `leader` | `leader` or `follower` |
| `FOLLOWER_URLS` | empty | Other node URLs, comma-separated (leader uses these for replication and read fan-out) |
| `WRITE_QUORUM_SIZE` | `1` | Write quorum W |
| `READ_QUORUM_SIZE` | `1` | Read quorum R |

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

## Load testing

`LoadTester` is a standalone `main` program (a multi-threaded HTTP load test) that lets you vary the read/write ratio and reports stale reads, throughput, and latency (avg / P99).

Adjust the parameters at the top of `LoadTester.java`:
- `WRITE_RATIO` — write ratio (e.g. `0.01 / 0.10 / 0.50 / 0.90`)
- `QUORUM_READ` — `true` hits `/kv` (quorum read, checks whether stale reads are eliminated); `false` hits `/local_read` (single node, observes inter-node lag)
- `THREADS` / `TOTAL_REQUESTS` / `KEY_POOL_SIZE`

With the cluster running:

```bash
cd node
./mvnw compile exec:java -Dexec.mainClass=com.kv.node.LoadTester
```

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
