package com.kv.node;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class Config {
    // 角色:"leader" 或 "follower"
    @Value("${ROLE:leader}")
    public String role;

    // follower 的地址列表,逗号分隔(只有 leader 用得上)
    // 例如:http://node2:8080,http://node3:8080,...
    @Value("${FOLLOWER_URLS:}")
    private String followerUrlsRaw;

    // 写quorum、读quorum
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