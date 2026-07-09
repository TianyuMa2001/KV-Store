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

    // Spring injects Config and RestClient automatically
    public KvController(Config config, RestClient restClient) {
        this.config = config;
        this.restClient = restClient;
    }

    // ========== Write: PUT /kv ==========
    @PutMapping("/kv")
    public ResponseEntity<Map<String, Object>> put(@RequestBody PutRequest req) throws InterruptedException {
        // Only the leader handles client writes (all writes go to the leader)
        if (!config.isLeader()) {
            return ResponseEntity.status(400).body(Map.of("error", "writes must go to leader"));
        }

        // 1. Leader assigns the version number
        VersionedValue existing = store.get(req.key);
        long newVersion = (existing == null) ? 1 : existing.version + 1;

        // 2. Leader writes to itself first; ack count starts at 1
        int acks = 1;
        store.put(req.key, new VersionedValue(req.value, newVersion));

        // 3. Replicate to followers one by one; stop once W acks are reached
        for (String followerUrl : config.followerUrls()) {
            if (acks >= config.writeQuorum) break;  // already have W, no need to send more
            try {
                ReplicateRequest rr = new ReplicateRequest();
                rr.key = req.key;
                rr.value = req.value;
                rr.version = newVersion;   // use the version assigned by the leader
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

        // 4. Leader sleeps 200ms (simulates the leader persisting after replicating to followers)
        Thread.sleep(200);

        // 5. Success once W is reached
        if (acks >= config.writeQuorum) {
            return ResponseEntity.status(201).body(Map.of("key", req.key, "version", newVersion));
        } else {
            return ResponseEntity.status(503).body(Map.of("error", "write quorum not reached"));
        }
    }

    // ========== Read: GET /kv/{key} ==========
    @GetMapping("/kv/{key}")
    public ResponseEntity<VersionedValue> get(@PathVariable String key) throws InterruptedException {
        Thread.sleep(50);  // read delay

        // R=1: read this node only
        if (config.readQuorum <= 1) {
            VersionedValue v = store.get(key);
            if (v == null) return ResponseEntity.notFound().build();
            return ResponseEntity.ok(v);
        }

        // R>1: read self + query (R-1) followers, take the highest version
        VersionedValue best = store.get(key);   // may be null
        int collected = 1;

        for (String followerUrl : config.followerUrls()) {
            if (collected >= config.readQuorum) break;
            try {
                VersionedValue v = restClient.get()
                        .uri(followerUrl + "/local_read/" + key)
                        .retrieve()
                        .body(VersionedValue.class);
                collected++;
                // keep the one with the higher version
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

    // ========== local_read: this node only (for testing, no sleep) ==========
    @GetMapping("/local_read/{key}")
    public ResponseEntity<VersionedValue> localRead(@PathVariable String key) {
        VersionedValue v = store.get(key);
        if (v == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(v);
    }

    // ========== replicate: follower receives replication from the leader (internal endpoint) ==========
    @PutMapping("/replicate")
    public ResponseEntity<Void> replicate(@RequestBody ReplicateRequest req) throws InterruptedException {
        Thread.sleep(200);  // follower also sleeps 200ms on a write
        store.put(req.key, new VersionedValue(req.value, req.version));  // use the version from the leader
        return ResponseEntity.status(201).build();
    }
}