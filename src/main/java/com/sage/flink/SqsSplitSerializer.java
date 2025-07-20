package com.sage.flink;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import java.io.IOException;

public class SqsSplitSerializer implements SimpleVersionedSerializer<SqsSplit> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(SqsSplit split) throws IOException {
        byte[] idBytes = split.splitId().getBytes("UTF-8");
        byte[] output = new byte[4 + idBytes.length];

        output[0] = (byte) (idBytes.length >> 24);
        output[1] = (byte) (idBytes.length >> 16);
        output[2] = (byte) (idBytes.length >> 8);
        output[3] = (byte) (idBytes.length);

        System.arraycopy(idBytes, 0, output, 4, idBytes.length);
        return output;
    }

    @Override
    public SqsSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported version: " + version);
        }

        int len = ((serialized[0] & 0xFF) << 24) |
                ((serialized[1] & 0xFF) << 16) |
                ((serialized[2] & 0xFF) << 8) |
                (serialized[3] & 0xFF);

        String id = new String(serialized, 4, len, "UTF-8");
        return new SqsSplit(id);
    }
}
