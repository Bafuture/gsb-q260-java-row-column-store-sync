package com.example.gsb.sync;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 同步组件统计：同步条数（含去重条数）、应用延迟分布、不一致条数、重建次数。
 */
public final class SyncMetrics {

    private static final long[] BUCKET_UPPER_BOUND_MS = {1, 5, 10, 50, 100, 500, 1000};
    private static final String[] BUCKET_LABELS = {
            "<=1ms", "<=5ms", "<=10ms", "<=50ms", "<=100ms", "<=500ms", "<=1000ms", ">1000ms"
    };

    private final AtomicLong appliedCount = new AtomicLong();
    private final AtomicLong duplicateCount = new AtomicLong();
    private final AtomicLong inconsistencyCount = new AtomicLong();
    private final AtomicLong rebuildCount = new AtomicLong();
    private final long[] bucketCounters = new long[BUCKET_LABELS.length];

    public void recordApplied(long latencyMillis) {
        appliedCount.incrementAndGet();
        int idx = BUCKET_LABELS.length - 1;
        for (int i = 0; i < BUCKET_UPPER_BOUND_MS.length; i++) {
            if (latencyMillis <= BUCKET_UPPER_BOUND_MS[i]) {
                idx = i;
                break;
            }
        }
        synchronized (bucketCounters) {
            bucketCounters[idx]++;
        }
    }

    public void recordDuplicate() {
        duplicateCount.incrementAndGet();
    }

    public void addInconsistencies(int n) {
        inconsistencyCount.addAndGet(n);
    }

    public void recordRebuild() {
        rebuildCount.incrementAndGet();
    }

    public long appliedCount() {
        return appliedCount.get();
    }

    public long duplicateCount() {
        return duplicateCount.get();
    }

    public long inconsistencyCount() {
        return inconsistencyCount.get();
    }

    public long rebuildCount() {
        return rebuildCount.get();
    }

    /** 延迟分布：桶标签 -> 条数，所有桶之和等于 appliedCount。 */
    public Map<String, Long> latencyDistribution() {
        Map<String, Long> dist = new LinkedHashMap<>();
        synchronized (bucketCounters) {
            for (int i = 0; i < BUCKET_LABELS.length; i++) {
                dist.put(BUCKET_LABELS[i], bucketCounters[i]);
            }
        }
        return dist;
    }
}
