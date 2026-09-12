package com.kv.node;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class Config {
    // Role: "leader" or "follower"
    @Value("${ROLE:leader}")
    public String role;

    // Comma-separated list of follower URLs (only the leader uses these)
    // e.g. http://node2:8080,http://node3:8080,...
    @Value("${FOLLOWER_URLS:}")
    private String followerUrlsRaw;

    @Value("${QUORUM_TIMEOUT_MS:1000}")
    public int timeoutMs = 1000;
    @Value("${REPLICATION_DELAY_MS:200}")
    public int replicationDelayMs = 200;
    @Value("${WRITE_DELAY_MS:200}")
    public int writeDelayMs = 200;
    @Value("${READ_DELAY_MS:50}")
    public int readDelayMs = 50;
    @Value("${FANOUT_THREADS:64}")
    public int fanoutThreads = 64;

    // Write quorum and read quorum
    @Value("${WRITE_QUORUM_SIZE:1}")
    public int writeQuorum;

    @Value("${READ_QUORUM_SIZE:1}")
    public int readQuorum;

    public List<String> followerUrls() {
        if (followerUrlsRaw == null || followerUrlsRaw.isBlank()) return new ArrayList<>();
        return Arrays.stream(followerUrlsRaw.split(",")).map(String::trim)
                .filter(s -> !s.isBlank()).distinct().toList();
    }

    public boolean isLeader() {
        return "leader".equalsIgnoreCase(role);
    }
}
