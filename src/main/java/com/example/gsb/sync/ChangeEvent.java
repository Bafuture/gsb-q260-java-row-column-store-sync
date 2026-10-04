package com.example.gsb.sync;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 一次行存变更。同一个主键的变更通过单调递增的 {@code seq} 标识先后顺序，
 * 列存按 seq 幂等应用，从而支持崩溃后的"重放但不重复应用"。
 */
public final class ChangeEvent {

    public enum Type {
        UPSERT,
        DELETE
    }

    public final long seq;
    public final long key;
    public final Type type;
    public final long timestampMillis;
    public final Map<String, String> fields;

    public ChangeEvent(long seq, long key, Type type, long timestampMillis,
                       Map<String, String> fields) {
        this.seq = seq;
        this.key = key;
        this.type = type;
        this.timestampMillis = timestampMillis;
        this.fields = fields == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    }

    public static ChangeEvent upsert(long seq, long key, Map<String, String> fields) {
        return new ChangeEvent(seq, key, Type.UPSERT, System.currentTimeMillis(), fields);
    }

    public static ChangeEvent delete(long seq, long key) {
        return new ChangeEvent(seq, key, Type.DELETE, System.currentTimeMillis(),
                Collections.emptyMap());
    }

    byte[] toBytes() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(out)) {
            dos.writeLong(seq);
            dos.writeLong(key);
            dos.writeByte((byte) type.ordinal());
            dos.writeLong(timestampMillis);
            dos.writeInt(fields.size());
            for (Map.Entry<String, String> e : fields.entrySet()) {
                dos.writeUTF(e.getKey());
                dos.writeUTF(e.getValue() == null ? "" : e.getValue());
            }
        } catch (IOException e) {
            throw new IllegalStateException("serialize event failed", e);
        }
        return out.toByteArray();
    }

    static ChangeEvent fromBytes(byte[] data) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {
            long seq = in.readLong();
            long key = in.readLong();
            Type type = Type.values()[in.readByte()];
            long ts = in.readLong();
            int n = in.readInt();
            Map<String, String> fields = new LinkedHashMap<>();
            for (int i = 0; i < n; i++) {
                fields.put(in.readUTF(), in.readUTF());
            }
            return new ChangeEvent(seq, key, type, ts, fields);
        } catch (IOException e) {
            throw new IllegalStateException("deserialize event failed", e);
        }
    }

    @Override
    public String toString() {
        return "ChangeEvent{seq=" + seq + ", key=" + key + ", type=" + type + "}";
    }
}
