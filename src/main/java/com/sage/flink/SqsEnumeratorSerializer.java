package com.sage.flink;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class SqsEnumeratorSerializer implements SimpleVersionedSerializer<Void> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(Void enumeratorCheckpoint) {
        return new byte[0];
    }

    @Override
    public Void deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported version: " + version);
        }
        return null;
    }
}
