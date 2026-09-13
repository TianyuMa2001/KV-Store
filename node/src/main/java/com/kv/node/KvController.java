package com.kv.node;

import jakarta.annotation.PreDestroy;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.HttpClientErrorException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** Single-leader, in-memory, version-ordered quorum prototype; not consensus. */
@RestController
public class KvController implements AutoCloseable {
    private final ConcurrentHashMap<String, VersionedValue> store = new ConcurrentHashMap<>();
    private final Config config;
    private final RestClient restClient;
    private final ThreadPoolExecutor fanout;
    private final AtomicLong failedReplications = new AtomicLong();

    public KvController(Config config, RestClient restClient) {
        this.config=config; this.restClient=restClient;
        int n=1+config.followerUrls().size();
        if (config.writeQuorum<1 || config.writeQuorum>n || config.readQuorum<1 || config.readQuorum>n
                || config.timeoutMs<=0 || config.fanoutThreads<1 || config.replicationDelayMs<0
                || config.writeDelayMs<0 || config.readDelayMs<0)
            throw new IllegalArgumentException("Invalid quorum, timeout, delay, or executor configuration");
        fanout=new ThreadPoolExecutor(config.fanoutThreads,config.fanoutThreads,0,TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1024), r->{Thread t=new Thread(r,"kv-fanout");t.setDaemon(true);return t;},
                new ThreadPoolExecutor.AbortPolicy());
    }

    private boolean valid(String key, String value) { return key!=null && !key.isBlank() && value!=null; }

    @PutMapping("/kv")
    public ResponseEntity<Map<String,Object>> put(@RequestBody PutRequest req) throws InterruptedException {
        if (!config.isLeader()) return ResponseEntity.status(400).body(Map.of("error","writes must go to leader"));
        if (req==null || !valid(req.key,req.value)) return ResponseEntity.badRequest().body(Map.of("error","key and value required"));
        long deadline=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(config.timeoutMs);
        // Atomic allocation + installation prevents duplicate versions and local rollback.
        VersionedValue current=store.compute(req.key,(key,old)->new VersionedValue(req.value,
                old==null ? 1 : Math.addExact(old.version,1)));
        CompletionService<Boolean> completion=new ExecutorCompletionService<>(fanout);
        int submitted=0;
        for (String peer:config.followerUrls()) {
            try {
                completion.submit(()->{
                    try {
                        ReplicateRequest rr=new ReplicateRequest();rr.key=req.key;rr.value=current.value;rr.version=current.version;
                        restClient.put().uri(peer+"/replicate").body(rr).retrieve().toBodilessEntity();
                        return true;
                    } catch (Exception ex) { failedReplications.incrementAndGet();return false; }
                }); submitted++;
            } catch (RejectedExecutionException ex) { failedReplications.incrementAndGet(); }
        }
        int acks=1,completed=0;
        while(acks<config.writeQuorum && completed<submitted) {
            Future<Boolean> result=completion.poll(Math.max(0,deadline-System.nanoTime()),TimeUnit.NANOSECONDS);
            if(result==null) break;
            completed++;
            try { if(result.get()) acks++; } catch(ExecutionException ex) { /* counts as failed */ }
        }
        // Work already submitted continues after the quorum response. No retry queue.
        Thread.sleep(config.writeDelayMs);
        Map<String,Object> response=new LinkedHashMap<>();
        response.put("key",req.key);response.put("version",current.version);response.put("acks",acks);
        response.put("required",config.writeQuorum);
        if(acks>=config.writeQuorum) { response.put("status","quorum_acknowledged");return ResponseEntity.status(201).body(response); }
        response.put("error","write quorum not reached");response.put("status","indeterminate");
        response.put("localApplied",true);
        return ResponseEntity.status(503).body(response);
    }

    private record ReplicaRead(boolean reachable, VersionedValue value) {}

    @GetMapping("/kv/{key}")
    public ResponseEntity<VersionedValue> get(@PathVariable String key) throws InterruptedException {
        Thread.sleep(config.readDelayMs);
        VersionedValue best=store.get(key);
        if(config.readQuorum==1) return best==null ? ResponseEntity.notFound().build() : ResponseEntity.ok(best);
        long deadline=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(config.timeoutMs);
        CompletionService<ReplicaRead> completion=new ExecutorCompletionService<>(fanout);
        int submitted=0;
        for(String peer:config.followerUrls()) {
            try {
                completion.submit(()->{
                    try {
                        VersionedValue value=restClient.get().uri(peer+"/local_read/{key}",key).retrieve().body(VersionedValue.class);
                        return new ReplicaRead(true,value);
                    } catch(HttpClientErrorException.NotFound ex) { return new ReplicaRead(true,null); }
                    catch(Exception ex) { return new ReplicaRead(false,null); }
                }); submitted++;
            } catch(RejectedExecutionException ex) { /* unavailable capacity */ }
        }
        int collected=1,completed=0;
        while(collected<config.readQuorum && completed<submitted) {
            Future<ReplicaRead> result=completion.poll(Math.max(0,deadline-System.nanoTime()),TimeUnit.NANOSECONDS);
            if(result==null) break;
            completed++;
            try {
                ReplicaRead read=result.get();
                if(read.reachable()) {
                    collected++;
                    if(read.value()!=null && (best==null || read.value().version>best.version)) best=read.value();
                }
            } catch(ExecutionException ex) { /* counts as unavailable */ }
        }
        if(collected<config.readQuorum) return ResponseEntity.status(503).build();
        return best==null ? ResponseEntity.notFound().build() : ResponseEntity.ok(best);
    }

    @GetMapping("/local_read/{key}")
    public ResponseEntity<VersionedValue> localRead(@PathVariable String key) {
        VersionedValue value=store.get(key);
        return value==null ? ResponseEntity.notFound().build() : ResponseEntity.ok(value);
    }

    @PutMapping("/replicate")
    public ResponseEntity<Void> replicate(@RequestBody ReplicateRequest req) throws InterruptedException {
        if(config.isLeader()) return ResponseEntity.status(403).build();
        if(req==null || !valid(req.key,req.value) || req.version<1) return ResponseEntity.badRequest().build();
        Thread.sleep(config.replicationDelayMs);
        VersionedValue kept=store.compute(req.key,(key,old)->old==null || req.version>old.version
                ? new VersionedValue(req.value,req.version) : old);
        if(kept.version==req.version && !Objects.equals(kept.value,req.value)) return ResponseEntity.status(409).build();
        return ResponseEntity.status(201).build();
    }

    @GetMapping("/health")
    public Map<String,Object> health() {
        return Map.of("status","up","role",config.role,"pid",ProcessHandle.current().pid(),
                "writeQuorum",config.writeQuorum,"readQuorum",config.readQuorum,
                "timeoutMs",config.timeoutMs,"replicationDelayMs",config.replicationDelayMs,
                "failedReplications",failedReplications.get(),
                "queuedTasks",fanout.getQueue().size(),"activeTasks",fanout.getActiveCount());
    }

    @PreDestroy public void close() { fanout.shutdownNow(); }
}
