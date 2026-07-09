package com.kv.node;

// A value plus its logical version number — the core data unit of the system.
// First write of a key gets version=1, each subsequent write increments it;
// when reading from multiple nodes, the highest version wins.
public class VersionedValue {
    public String value;
    public long version;

    public VersionedValue() {}  // empty constructor needed for Spring JSON deserialization
    public VersionedValue(String value, long version) {
        this.value = value;
        this.version = version;
    }
}