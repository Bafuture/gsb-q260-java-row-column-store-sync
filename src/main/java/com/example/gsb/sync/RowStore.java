package com.example.gsb.sync;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 行式存储：按主键的插入、更新与删除，均为 O(1) 点操作。
 * 删除保留带时间戳的墓碑，供一致性检查区分"刚删还没同步"与"列存多出来的脏数据"。
 */
public final class RowStore {

    public record RowRecord(long key, Map<String, String> fields, long lastSeq,
                            long lastModifiedMillis) {
    }

    public record Tombstone(long key, long seq, long deletedAtMillis) {
    }

    private final ConcurrentHashMap<Long, RowRecord> rows = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Tombstone> tombstones = new ConcurrentHashMap<>();

    /** 应用一条变更（写入路径与崩溃后 WAL 重放共用）。 */
    public void apply(ChangeEvent event) {
        if (event.type == ChangeEvent.Type.UPSERT) {
            rows.put(event.key, new RowRecord(event.key, event.fields, event.seq,
                    event.timestampMillis));
            tombstones.remove(event.key);
        } else {
            rows.remove(event.key);
            tombstones.put(event.key,
                    new Tombstone(event.key, event.seq, event.timestampMillis));
        }
    }

    public Optional<RowRecord> get(long key) {
        return Optional.ofNullable(rows.get(key));
    }

    public Optional<Tombstone> tombstone(long key) {
        return Optional.ofNullable(tombstones.get(key));
    }

    public List<RowRecord> rowsInRange(long fromKeyInclusive, long toKeyInclusive) {
        List<RowRecord> result = new ArrayList<>();
        for (RowRecord r : rows.values()) {
            if (r.key() >= fromKeyInclusive && r.key() <= toKeyInclusive) {
                result.add(r);
            }
        }
        result.sort((a, b) -> Long.compare(a.key(), b.key()));
        return result;
    }

    public List<Long> keysInRange(long fromKeyInclusive, long toKeyInclusive) {
        List<Long> result = new ArrayList<>();
        for (RowRecord r : rowsInRange(fromKeyInclusive, toKeyInclusive)) {
            result.add(r.key());
        }
        return result;
    }

    /** 清理过老的墓碑；老到超过保留期仍未同步到列存会被检查器当作真实不一致。 */
    public void pruneTombstones(long olderThanMillis) {
        long cutoff = System.currentTimeMillis() - olderThanMillis;
        tombstones.values().removeIf(t -> t.deletedAtMillis() < cutoff);
    }

    public int size() {
        return rows.size();
    }
}
