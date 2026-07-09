package com.kv.node;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadTester {

    // ===== 可调参数 =====
    static final int THREADS = 16;
    static final int TOTAL_REQUESTS = 2000;
    static final int KEY_POOL_SIZE = 20;
    static final double WRITE_RATIO = 0.9;     // 改这个测四种读写比:0.01/0.10/0.50/0.90

    // 读模式开关:
    //  true  = quorum读(打/kv,该节点按R收集多节点取最新)—— 测quorum能否消除stale
    //  false = local读(打/local_read,直接看单节点)—— 测节点间滞后程度
    static final boolean QUORUM_READ = true;

    static final String LEADER = "http://localhost:8080";
    static final String[] ALL_NODES = {
            "http://localhost:8080", "http://localhost:8081", "http://localhost:8082",
            "http://localhost:8083", "http://localhost:8084"
    };

    // ===== 共享统计 =====
    static final ConcurrentHashMap<String, Long> lastWrittenVersion = new ConcurrentHashMap<>();
    static final AtomicInteger staleReads = new AtomicInteger(0);
    static final AtomicInteger totalReads = new AtomicInteger(0);
    static final AtomicInteger totalWrites = new AtomicInteger(0);
    static final AtomicInteger errors = new AtomicInteger(0);
    static final List<Long> writeLatencies = Collections.synchronizedList(new ArrayList<>());
    static final List<Long> readLatencies = Collections.synchronizedList(new ArrayList<>());
    static final AtomicInteger readNodeCounter = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);

        System.out.println("开始压测: " + TOTAL_REQUESTS + " 请求, " + THREADS + " 线程, 写比例 "
                + WRITE_RATIO + ", 读模式=" + (QUORUM_READ ? "quorum(/kv)" : "local(/local_read)"));
        long startTime = System.currentTimeMillis();

        AtomicInteger done = new AtomicInteger(0);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < TOTAL_REQUESTS; i++) {
            futures.add(pool.submit(() -> {
                String key = "key" + ThreadLocalRandom.current().nextInt(KEY_POOL_SIZE);
                boolean isWrite = ThreadLocalRandom.current().nextDouble() < WRITE_RATIO;
                try {
                    if (isWrite) doWrite(client, key);
                    else doRead(client, key);
                } catch (Exception e) {
                    errors.incrementAndGet();
                }
                int d = done.incrementAndGet();
                if (d % 200 == 0) System.out.println("已完成 " + d + " / " + TOTAL_REQUESTS);
            }));
        }

        for (Future<?> f : futures) f.get();
        pool.shutdown();

        long wallTime = System.currentTimeMillis() - startTime;
        printReport(wallTime);
    }

    static void doWrite(HttpClient client, String key) throws Exception {
        String body = "{\"key\":\"" + key + "\",\"value\":\"v" + System.nanoTime() + "\"}";
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(LEADER + "/kv"))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(15))
                .PUT(HttpRequest.BodyPublishers.ofString(body))
                .build();

        long t0 = System.nanoTime();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        long latency = System.nanoTime() - t0;
        writeLatencies.add(latency);
        totalWrites.incrementAndGet();

        long version = parseVersion(resp.body());
        if (version > 0) lastWrittenVersion.merge(key, version, Math::max);
    }

    static void doRead(HttpClient client, String key) throws Exception {
        // 选节点:quorum读也轮流打不同节点当协调者;local读直接戳单节点
        String node = ALL_NODES[readNodeCounter.getAndIncrement() % ALL_NODES.length];
        String path = QUORUM_READ ? ("/kv/" + key) : ("/local_read/" + key);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(node + path))
                .timeout(Duration.ofSeconds(15))
                .GET()
                .build();

        long t0 = System.nanoTime();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        long latency = System.nanoTime() - t0;
        readLatencies.add(latency);
        totalReads.incrementAndGet();

        Long expectedVersion = lastWrittenVersion.get(key);
        if (expectedVersion == null) return;

        if (resp.statusCode() == 404) {
            staleReads.incrementAndGet();
        } else {
            long readVersion = parseVersion(resp.body());
            if (readVersion < expectedVersion) staleReads.incrementAndGet();
        }
    }

    static long parseVersion(String json) {
        if (json == null) return -1;
        int idx = json.indexOf("\"version\":");
        if (idx < 0) return -1;
        int start = idx + 10, end = start;
        while (end < json.length() && Character.isDigit(json.charAt(end))) end++;
        try { return Long.parseLong(json.substring(start, end).trim()); }
        catch (Exception e) { return -1; }
    }

    static void printReport(long wallTime) {
        System.out.println("\n========== LOAD TEST REPORT ==========");
        System.out.println("读模式: " + (QUORUM_READ ? "quorum(/kv)" : "local(/local_read)"));
        System.out.println("总请求数: " + (totalReads.get() + totalWrites.get()));
        System.out.println("写请求: " + totalWrites.get() + " | 读请求: " + totalReads.get() + " | 错误: " + errors.get());
        System.out.println("总耗时: " + wallTime + " ms | 吞吐量: "
                + String.format("%.1f", (totalReads.get() + totalWrites.get()) * 1000.0 / wallTime) + " req/s");
        int reads = Math.max(1, totalReads.get());
        System.out.println("Stale reads: " + staleReads.get() + " / " + totalReads.get()
                + " (" + String.format("%.1f", staleReads.get() * 100.0 / reads) + "%)");
        System.out.println("写延迟 平均: " + avgMs(writeLatencies) + " ms, P99: " + p99Ms(writeLatencies) + " ms");
        System.out.println("读延迟 平均: " + avgMs(readLatencies) + " ms, P99: " + p99Ms(readLatencies) + " ms");
        System.out.println("======================================");
    }

    static String avgMs(List<Long> lat) {
        if (lat.isEmpty()) return "0";
        double sum = 0;
        synchronized (lat) { for (long l : lat) sum += l; }
        return String.format("%.1f", sum / lat.size() / 1_000_000);
    }

    static String p99Ms(List<Long> lat) {
        if (lat.isEmpty()) return "0";
        List<Long> copy;
        synchronized (lat) { copy = new ArrayList<>(lat); }
        Collections.sort(copy);
        int idx = Math.min(copy.size() - 1, (int) (copy.size() * 0.99));
        return String.format("%.1f", copy.get(idx) / 1_000_000.0);
    }
}