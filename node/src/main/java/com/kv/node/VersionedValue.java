package com.kv.node;

// 一个值 + 它的逻辑版本号。这是整个系统的核心数据单元。
// 作业要求:每个 key 第一次写 version=1,之后每次写 +1,读多节点时取 version 最大的。
public class VersionedValue {
    public String value;
    public long version;

    public VersionedValue() {}  // Spring 转 JSON 需要空构造器
    public VersionedValue(String value, long version) {
        this.value = value;
        this.version = version;
    }
}