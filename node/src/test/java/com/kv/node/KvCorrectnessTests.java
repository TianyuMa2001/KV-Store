package com.kv.node;

import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class KvCorrectnessTests {
    static Config config(String role, int w, int r, String peers) throws Exception {
        Config c = new Config(); c.role=role; c.writeQuorum=w; c.readQuorum=r;
        Field f=Config.class.getDeclaredField("followerUrlsRaw"); f.setAccessible(true); f.set(c,peers);
        return c;
    }
    static PutRequest put(String value) { PutRequest p=new PutRequest(); p.key="key"; p.value=value; return p; }
    static ReplicateRequest replica(long version, String value) {
        ReplicateRequest p=new ReplicateRequest(); p.key="key"; p.value=value; p.version=version; return p;
    }

    @Test void olderReplicationCannotOverwriteNewerValue() throws Exception {
        KvController c=new KvController(config("follower",1,1,""), RestClient.create());
        try {
            c.replicate(replica(2,"new")); c.replicate(replica(1,"old"));
            assertEquals(2,c.localRead("key").getBody().version);
            assertEquals("new",c.localRead("key").getBody().value);
        } finally { close(c); }
    }
    @Test void unavailableReadQuorumMustFail() throws Exception {
        KvController c=new KvController(config("leader",1,2,"http://127.0.0.1:1"),RestClient.create());
        try { assertEquals(503,c.get("key").getStatusCode().value()); } finally { close(c); }
    }
    @Test void sameKeyVersionsAreUnique() throws Exception {
        KvController c=new KvController(config("leader",1,1,""),RestClient.create());
        ExecutorService pool=Executors.newFixedThreadPool(16);
        try {
            List<Callable<Long>> tasks=new ArrayList<>();
            for(int i=0;i<256;i++) { final int n=i; tasks.add(()-> ((Number)c.put(put("v"+n)).getBody().get("version")).longValue()); }
            Set<Long> versions=new HashSet<>();
            for(Future<Long> result:pool.invokeAll(tasks)) versions.add(result.get());
            assertEquals(256,versions.size()); assertEquals(256,c.localRead("key").getBody().version);
        } finally { pool.shutdownNow(); close(c); }
    }
    static void close(KvController c) throws Exception {
        // Reflection keeps the regression suite runnable on the baseline commit.
        if(c instanceof AutoCloseable) ((AutoCloseable)c).close();
    }
}
