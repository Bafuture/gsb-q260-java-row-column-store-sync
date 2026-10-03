package com.example.gsb.sync;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 同步组件运行统计：同步条数、延迟分布、不一致条数、重建次数。
 */
public class SyncStats {

  /** 延迟分桶上界（毫秒），最后一桶为 +Inf。 */
  private static final long[] LATENCY_BUCKETS_MS = {1, 10, 100, 1000};

  private final LongAdder syncedCount = new LongAdder();
  private final LongAdder duplicateSkipped = new LongAdder();
  private final LongAdder mismatchCount = new LongAdder();
  private final LongAdder rebuildCount = new LongAdder();
  private final LongAdder latencyCount = new LongAdder();
  private final LongAdder latencySumMillis = new LongAdder();
  private final AtomicLong latencyMaxMillis = new AtomicLong(0);
  private final LongAdder[] latencyBuckets = new LongAdder[LATENCY_BUCKETS_MS.length + 1];

  public SyncStats() {
    for (int i = 0; i < latencyBuckets.length; i++) {
      latencyBuckets[i] = new LongAdder();
    }
  }

  public void recordSync(long latencyMillis) {
    syncedCount.increment();
    latencyCount.increment();
    latencySumMillis.add(latencyMillis);
    latencyMaxMillis.accumulateAndGet(latencyMillis, Math::max);
    int bucket = 0;
    while (bucket < LATENCY_BUCKETS_MS.length && latencyMillis > LATENCY_BUCKETS_MS[bucket]) {
      bucket++;
    }
    latencyBuckets[bucket].increment();
  }

  /** 队列重投但列存按 seq 判定为重复而跳过。 */
  public void recordDuplicateSkipped() {
    duplicateSkipped.increment();
  }

  /** 一次一致性检查发现的不一致条数（累加）。 */
  public void recordMismatches(int count) {
    mismatchCount.add(count);
  }

  public void recordRebuild() {
    rebuildCount.increment();
  }

  /** 统计快照。 */
  public record Snapshot(long syncedCount, long duplicateSkipped, long mismatchCount,
                         long rebuildCount, long latencyCount, double avgLatencyMillis,
                         long maxLatencyMillis, Map<String, Long> latencyBuckets) {
  }

  public Snapshot snapshot() {
    Map<String, Long> buckets = new LinkedHashMap<>();
    for (int i = 0; i < latencyBuckets.length; i++) {
      String label = i == 0 ? "<=" + LATENCY_BUCKETS_MS[0] + "ms"
          : i == latencyBuckets.length - 1 ? ">" + LATENCY_BUCKETS_MS[LATENCY_BUCKETS_MS.length - 1] + "ms"
          : "<=" + LATENCY_BUCKETS_MS[i] + "ms";
      buckets.put(label, latencyBuckets[i].sum());
    }
    long count = latencyCount.sum();
    double avg = count == 0 ? 0.0 : (double) latencySumMillis.sum() / count;
    return new Snapshot(syncedCount.sum(), duplicateSkipped.sum(), mismatchCount.sum(),
        rebuildCount.sum(), count, avg, latencyMaxMillis.get(), Map.copyOf(buckets));
  }
}
