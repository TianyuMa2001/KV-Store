package com.kv.node;

// 客户端 PUT 时发来的 body
class PutRequest {
    public String key;
    public String value;
}

// Leader 内部把写同步给 Follower 时,要连版本号一起发过去,
// 因为作业规定"只有 Leader 分配版本号",Follower 不能自己算。
class ReplicateRequest {
    public String key;
    public String value;
    public long version;
}