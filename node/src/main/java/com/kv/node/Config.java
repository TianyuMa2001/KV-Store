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

    // Write quorum and read quorum
    @Value("${WRITE_QUORUM_SIZE:1}")
    public int writeQuorum;

    @Value("${READ_QUORUM_SIZE:1}")
    public int readQuorum;

    public List<String> followerUrls() {
        if (followerUrlsRaw == null || followerUrlsRaw.isBlank()) return new ArrayList<>();
        return Arrays.asList(followerUrlsRaw.split(","));
    }

    public boolean isLeader() {
        return "leader".equalsIgnoreCase(role);
    }
}