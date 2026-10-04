package com.example.gsb.sync;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * 一致性检查器：比对 [fromKey, toKey] 范围内行存与列存，输出不一致的主键与字段。
 *
 * <p>判定窗口（避免把正常异步延迟误判为不一致）：满足以下任一条件的主键本轮跳过，
 * 计入 {@link ConsistencyReport#skippedInFlight()}：
 * <ol>
 *   <li>行存该主键最新变更的 seq 尚未被同步引擎消费（seq &gt; 当前 ack）——事件还在队列里；</li>
 *   <li>行存该主键最后修改时间距今小于 {@code lagToleranceMillis}——变更刚发生；</li>
 *   <li>该主键所在列存批次正处于 {@link ColumnStore.BatchState#REBUILDING}——
 *       重建期间旧批次滞后属于正常状态。</li>
 * </ol>
 * 越过窗口仍未一致的，才输出为真实不一致。
 */
public final class ConsistencyChecker {

    public enum Kind {
        MISSING_IN_COLUMN,
        MISSING_IN_ROW,
        FIELD_MISMATCH
    }

    public record Inconsistency(long key, Kind kind,
                                Map<String, String[]> fieldDiffs) {
        /** fieldDiffs: 字段名 -> [行存值, 列存值]，null 表示该侧缺失。 */
    }

    public record ConsistencyReport(List<Inconsistency> inconsistencies,
                                    int skippedInFlight,
                                    int compared) {
        public boolean isConsistent() {
            return inconsistencies.isEmpty();
        }
    }

    private final RowStore rowStore;
    private final ColumnStore columnStore;
    private final SyncQueue queue;
    private final SyncMetrics metrics;
    private final long lagToleranceMillis;
    private final long tombstoneRetentionMillis;

    public ConsistencyChecker(RowStore rowStore, ColumnStore columnStore, SyncQueue queue,
                              SyncMetrics metrics, long lagToleranceMillis,
                              long tombstoneRetentionMillis) {
        this.rowStore = rowStore;
        this.columnStore = columnStore;
        this.queue = queue;
        this.metrics = metrics;
        this.lagToleranceMillis = lagToleranceMillis;
        this.tombstoneRetentionMillis = tombstoneRetentionMillis;
    }

    public ConsistencyReport check(long fromKeyInclusive, long toKeyInclusive) {
        rowStore.pruneTombstones(tombstoneRetentionMillis);
        long now = System.currentTimeMillis();
        long ack = queue.ackSeq();

        TreeSet<Long> keys = new TreeSet<>();
        keys.addAll(rowStore.keysInRange(fromKeyInclusive, toKeyInclusive));
        Map<Long, ColumnStore.Entry> columnLive =
                columnStore.liveEntriesInRange(fromKeyInclusive, toKeyInclusive);
        keys.addAll(columnLive.keySet());

        List<Inconsistency> diffs = new ArrayList<>();
        int skipped = 0;
        int compared = 0;

        for (long key : keys) {
            if (columnStore.stateOf(key) == ColumnStore.BatchState.REBUILDING) {
                skipped++;
                continue;
            }
            RowStore.RowRecord row = rowStore.get(key).orElse(null);
            RowStore.Tombstone tombstone = rowStore.tombstone(key).orElse(null);
            ColumnStore.Entry column = columnLive.get(key);

            long rowSeq = row != null ? row.lastSeq() : (tombstone != null ? tombstone.seq() : -1L);
            long rowTime = row != null ? row.lastModifiedMillis()
                    : (tombstone != null ? tombstone.deletedAtMillis() : 0L);

            if (rowSeq > ack) {
                skipped++; // 事件还在同步队列里，属正常延迟
                continue;
            }
            if (rowSeq >= 0 && now - rowTime < lagToleranceMillis) {
                skipped++; // 刚变更，处于判定窗口内
                continue;
            }
            compared++;

            if (row != null && column == null) {
                diffs.add(new Inconsistency(key, Kind.MISSING_IN_COLUMN, Map.of()));
            } else if (row == null && column != null) {
                diffs.add(new Inconsistency(key, Kind.MISSING_IN_ROW, Map.of()));
            } else if (row != null) {
                Map<String, String[]> fieldDiffs = diffFields(row.fields(), column.fields());
                if (!fieldDiffs.isEmpty()) {
                    diffs.add(new Inconsistency(key, Kind.FIELD_MISMATCH, fieldDiffs));
                }
            }
        }

        metrics.addInconsistencies(diffs.size());
        return new ConsistencyReport(diffs, skipped, compared);
    }

    private static Map<String, String[]> diffFields(Map<String, String> rowFields,
                                                    Map<String, String> columnFields) {
        Map<String, String[]> diffs = new LinkedHashMap<>();
        TreeSet<String> names = new TreeSet<>();
        names.addAll(rowFields.keySet());
        names.addAll(columnFields.keySet());
        for (String name : names) {
            String rowVal = rowFields.get(name);
            String colVal = columnFields.get(name);
            if (rowVal == null ? colVal != null : !rowVal.equals(colVal)) {
                diffs.put(name, new String[] {rowVal, colVal});
            }
        }
        return diffs;
    }
}
