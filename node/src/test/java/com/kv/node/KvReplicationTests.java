package com.kv.node;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class KvReplicationTests {
    static class Peer implements AutoCloseable {
        final HttpServer server;
        final ExecutorService pool=Executors.newCachedThreadPool();
        final CountDownLatch entered=new CountDownLatch(1), applied=new CountDownLatch(1);
        Peer(CountDownLatch release, int code, String body) throws Exception {
            server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);server.setExecutor(pool);
            server.createContext("/",exchange->{
                exchange.getRequestBody().readAllBytes();entered.countDown();
                try {
                    if(release!=null) release.await(3,TimeUnit.SECONDS);
                    byte[] bytes=body.getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type","application/json");
                    exchange.sendResponseHeaders(code,bytes.length==0 ? -1:bytes.length);
                    if(bytes.length>0) exchange.getResponseBody().write(bytes);
                    applied.countDown();
                } catch(InterruptedException ex) { Thread.currentThread().interrupt(); }
                finally {exchange.close();}
            });server.start();
        }
        String url(){return "http://127.0.0.1:"+server.getAddress().getPort();}
        public void close(){server.stop(0);pool.shutdownNow();}
    }
    static KvController leader(int w,int r,String peers) throws Exception {
        Config c=KvCorrectnessTests.config("leader",w,r,peers);
        c.writeDelayMs=0;c.readDelayMs=0;c.timeoutMs=300;
        return new KvController(c,new NodeApplication().restClient(c));
    }

    @Test void slowPeerDoesNotBlockFastQuorumAndStillReceivesWrite() throws Exception {
        CountDownLatch release=new CountDownLatch(1);
        try(Peer slow=new Peer(release,201,"");Peer fast=new Peer(null,201,"");
            KvController c=leader(2,1,slow.url()+","+fast.url())) {
            ExecutorService caller=Executors.newSingleThreadExecutor();
            try {
                Future<Integer> status=caller.submit(()->c.put(KvCorrectnessTests.put("v")).getStatusCode().value());
                assertTrue(slow.entered.await(1,TimeUnit.SECONDS));
                assertEquals(201,status.get(1,TimeUnit.SECONDS));
                assertEquals(1,slow.applied.getCount());
                release.countDown();assertTrue(slow.applied.await(1,TimeUnit.SECONDS));
            } finally {release.countDown();caller.shutdownNow();}
        }
    }
    @Test void wOneStillReplicatesToAllPeers() throws Exception {
        try(Peer p=new Peer(null,201,"");Peer q=new Peer(null,201,"");
            KvController c=leader(1,1,p.url()+","+q.url())) {
            assertEquals(201,c.put(KvCorrectnessTests.put("v")).getStatusCode().value());
            assertTrue(p.applied.await(1,TimeUnit.SECONDS));assertTrue(q.applied.await(1,TimeUnit.SECONDS));
        }
    }
    @Test void timeoutIsExplicitPartialWrite() throws Exception {
        CountDownLatch release=new CountDownLatch(1);
        try(Peer p=new Peer(release,201,"");KvController c=leader(2,1,p.url())) {
            long start=System.nanoTime();var response=c.put(KvCorrectnessTests.put("v"));
            assertEquals(503,response.getStatusCode().value());
            assertEquals("indeterminate",response.getBody().get("status"));
            assertEquals(true,response.getBody().get("localApplied"));
            assertEquals("v",c.localRead("key").getBody().value);
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-start)<1500);
            release.countDown();
        } finally {release.countDown();}
    }
    @Test void missingKeyResponsesCountTowardReadQuorum() throws Exception {
        try(Peer p=new Peer(null,404,"");KvController c=leader(1,2,p.url())) {
            assertEquals(404,c.get("absent").getStatusCode().value());
        }
    }
    @Test void readChoosesNewestVersionAcrossStaleReplicas() throws Exception {
        try(Peer p=new Peer(null,200,"{\"value\":\"new\",\"version\":10}");
            Peer q=new Peer(null,200,"{\"value\":\"old\",\"version\":2}");
            KvController c=leader(1,3,p.url()+","+q.url())) {
            assertEquals(10,c.get("key").getBody().version);
        }
    }
    @Test void conflictingVersionAndInvalidInputsAreRejected() throws Exception {
        Config cfg=KvCorrectnessTests.config("follower",1,1,"");cfg.replicationDelayMs=0;
        try(KvController c=new KvController(cfg,RestClient.create())) {
            assertEquals(201,c.replicate(KvCorrectnessTests.replica(2,"a")).getStatusCode().value());
            assertEquals(409,c.replicate(KvCorrectnessTests.replica(2,"b")).getStatusCode().value());
            assertEquals(201,c.replicate(KvCorrectnessTests.replica(2,"a")).getStatusCode().value());
            assertEquals(400,c.replicate(KvCorrectnessTests.replica(0,"a")).getStatusCode().value());
            assertEquals(400,c.put(KvCorrectnessTests.put("v")).getStatusCode().value());
        }
        try(KvController c=leader(1,1,"")) {
            assertEquals(400,c.put(new PutRequest()).getStatusCode().value());
            assertEquals(403,c.replicate(KvCorrectnessTests.replica(2,"a")).getStatusCode().value());
        }
    }
}
