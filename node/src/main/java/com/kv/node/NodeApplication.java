package com.kv.node;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestClient;

@SpringBootApplication
public class NodeApplication {
    public static void main(String[] args) {
        SpringApplication.run(NodeApplication.class, args);
    }

    // 提供一个 RestClient,Leader 用它向 Follower 发请求
    @Bean
    public RestClient restClient() {
        return RestClient.create();
    }
}