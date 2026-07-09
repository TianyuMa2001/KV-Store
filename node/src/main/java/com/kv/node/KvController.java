package com.kv.node;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClient;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RestController
public class KvController {

    private final ConcurrentHashMap<String, VersionedValue> store = new ConcurrentHashMap<>();
    private final Config config;
    private final RestClient restClient;

    // Spring 会自动把 Config 和 RestClient 注入进来
    public KvController(Config config, RestClient restClient) {
        this.config = config;
        this.restClient = restClient;
    }

    // ========== 写:PUT /kv ==========
    @PutMapping("/kv")
    public ResponseEntity<Map<String, Object>> put(@RequestBody PutRequest req) throws InterruptedException {
        // 只有 Leader 处理客户端写(作业规定写全发给 Leader)
        if (!config.isLeader()) {
            return ResponseEntity.status(400).body(Map.of("error", "writes must go to leader"));
        }

        // 1. Leader 分配版本号
        VersionedValue existing = store.get(req.key);
        long newVersion = (existing == null) ? 1 : existing.version + 1;

        // 2. Leader 先写自己,成功数从 1 起算
        int acks = 1;
        store.put(req.key, new VersionedValue(req.value, newVersion));

        // 3. 挨个同步给 Follower,直到达到 W 个确认就停止发送剩下的
        for (String followerUrl : config.followerUrls()) {
            if (acks >= config.writeQuorum) break;  // 已够 W 个,不用再发
            try {
                ReplicateRequest rr = new ReplicateRequest();
                rr.key = req.key;
                rr.value = req.value;
                rr.version = newVersion;   // 用 Leader 分配的版本号
                restClient.put()
                        .uri(followerUrl + "/replicate")
                        .body(rr)
                        .retrieve()
                        .toBodilessEntity();
                acks++;
            } catch (Exception e) {
                System.out.println("replicate to " + followerUrl + " failed: " + e.getMessage());
            }
        }

        // 4. Leader 自己 sleep 200ms(作业规定:同步完 Follower 后,Leader 模拟自己落盘)
        Thread.sleep(200);

        // 5. 达到 W 就成功
        if (acks >= config.writeQuorum) {
            return ResponseEntity.status(201).body(Map.of("key", req.key, "version", newVersion));
        } else {
            return ResponseEntity.status(503).body(Map.of("error", "write quorum not reached"));
        }
    }

    // ========== 读:GET /kv/{key} ==========
    @GetMapping("/kv/{key}")
    public ResponseEntity<VersionedValue> get(@PathVariable String key) throws InterruptedException {
        Thread.sleep(50);  // 读延迟

        // R=1:只读本节点
        if (config.readQuorum <= 1) {
            VersionedValue v = store.get(key);
            if (v == null) return ResponseEntity.notFound().build();
            return ResponseEntity.ok(v);
        }

        // R>1:读自己 + 问 (R-1) 个 Follower,取版本号最大的
        VersionedValue best = store.get(key);   // 可能为 null
        int collected = 1;

        for (String followerUrl : config.followerUrls()) {
            if (collected >= config.readQuorum) break;
            try {
                VersionedValue v = restClient.get()
                        .uri(followerUrl + "/local_read/" + key)
                        .retrieve()
                        .body(VersionedValue.class);
                collected++;
                // 取版本号更大的
                if (v != null && (best == null || v.version > best.version)) {
                    best = v;
                }
            } catch (Exception e) {
                System.out.println("read from " + followerUrl + " failed: " + e.getMessage());
            }
        }

        if (best == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(best);
    }

    // ========== local_read:只看本节点(测试用,不 sleep)==========
    @GetMapping("/local_read/{key}")
    public ResponseEntity<VersionedValue> localRead(@PathVariable String key) {
        VersionedValue v = store.get(key);
        if (v == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(v);
    }

    // ========== replicate:Follower 接收 Leader 同步(内部端点)==========
    @PutMapping("/replicate")
    public ResponseEntity<Void> replicate(@RequestBody ReplicateRequest req) throws InterruptedException {
        Thread.sleep(200);  // Follower 收到写也 sleep 200ms
        store.put(req.key, new VersionedValue(req.value, req.version));  // 用 Leader 给的版本号
        return ResponseEntity.status(201).build();
    }
}