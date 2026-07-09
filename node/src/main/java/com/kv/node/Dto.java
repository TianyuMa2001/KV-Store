package com.kv.node;

// Body sent by the client on a PUT
class PutRequest {
    public String key;
    public String value;
}

// When the leader replicates a write to a follower, it sends the version too,
// because only the leader assigns version numbers — followers can't compute their own.
class ReplicateRequest {
    public String key;
    public String value;
    public long version;
}