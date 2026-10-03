package com.example.gsb.sync;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * 列式存储：主键按固定大小划分为若干批次（batch），每个批次独立应用变更、独立重建。
 *
 * <p>关键语义：
 * <ul>
 *   <li>幂等：每个批次记录已应用水位 {@code appliedSeq}，seq 不高于水位的事件直接跳过；</li>
 *   <li>重建：重建期间旧批次数据继续对外提供查询，状态标记为 {@link BatchState#REBUILDING}；
 *       重建期间到达的变更先挂起（deferred），快照换入时合并，保证不丢更新；</li>
 *   <li>快照读：批次数据以不可变 Map 整体换入换出，查询与一致性检查读到的始终是某个一致版本。</li>
 * </ul>
 */
public class ColumnStore {

  public enum BatchState {
    ACTIVE,
    REBUILDING
  }

  /** 批次对外状态视图。 */
  public record BatchStatus(long batchId, BatchState state, long appliedSeq, int version, int rowCount) {
  }

  /** 单个批次。 */
  public static final class Batch {
    private final long id;
    private volatile Map<Long, Map<String, Object>> data = Map.of();
    private volatile BatchState state = BatchState.ACTIVE;
    private volatile long appliedSeq = 0L;
    private volatile int version = 0;
    private final List<ChangeEvent> deferred = new ArrayList<>();

    Batch(long id) {
      this.id = id;
    }

    public long id() {
      return id;
    }

    public BatchState state() {
      return state;
    }

    public long appliedSeq() {
      return appliedSeq;
    }

    public int version() {
      return version;
    }

    /** 当前数据快照（不可变引用，可安全遍历）。 */
    public Map<Long, Map<String, Object>> data() {
      return data;
    }

    public Map<String, Object> get(long pk) {
      return data.get(pk);
    }
  }

  private final long batchSize;
  private final ConcurrentHashMap<Long, Batch> batches = new ConcurrentHashMap<>();
  private final SyncStats stats;
  /** 测试钩子：重建快照换入前执行，用于在“重建中”状态暂停。 */
  private volatile Runnable rebuildBarrier = () -> { };

  public ColumnStore(long batchSize, SyncStats stats) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize 必须为正数");
    }
    this.batchSize = batchSize;
    this.stats = stats;
  }

  public long batchOf(long pk) {
    return Math.floorDiv(pk, batchSize);
  }

  /** 批次覆盖的主键区间 [lo, hi]。 */
  public long[] batchRange(long batchId) {
    long lo = Math.multiplyExact(batchId, batchSize);
    return new long[] {lo, lo + batchSize - 1};
  }

  public Batch batch(long pk) {
    return batches.get(batchOf(pk));
  }

  public Batch batchById(long batchId) {
    return batches.get(batchId);
  }

  public List<Batch> batches() {
    return List.copyOf(batches.values());
  }

  public Optional<Map<String, Object>> get(long pk) {
    Batch batch = batches.get(batchOf(pk));
    return batch == null ? Optional.empty() : Optional.ofNullable(batch.get(pk));
  }

  public BatchStatus status(long pk) {
    Batch batch = batches.get(batchOf(pk));
    if (batch == null) {
      return new BatchStatus(batchOf(pk), BatchState.ACTIVE, 0L, 0, 0);
    }
    return new BatchStatus(batch.id(), batch.state(), batch.appliedSeq(), batch.version(),
        batch.data().size());
  }

  /**
   * 应用一条变更。返回 true 表示真正落盘；false 表示按 seq 判定为重复而跳过。
   * 重建中的批次不直接改旧数据，而是挂起到 deferred，待快照换入时合并。
   */
  public boolean apply(ChangeEvent event) {
    Batch batch = batches.computeIfAbsent(batchOf(event.pk()), Batch::new);
    synchronized (batch) {
      if (event.seq() <= batch.appliedSeq) {
        return false;
      }
      if (batch.state == BatchState.REBUILDING) {
        batch.deferred.add(event);
      } else {
        batch.data = applyTo(batch.data, event);
      }
      batch.appliedSeq = event.seq();
      return true;
    }
  }

  /**
   * 重建指定批次：标记 REBUILDING → 取行存快照 → 合并重建期间挂起的变更 → 原子换入。
   *
   * @param snapshotSupplier 返回该批次主键范围内的行存快照
   * @param snapshotSeq 快照拍摄前读取的行存 seq 水位（快照之后的变更由队列事件补齐）
   */
  public void rebuild(long batchId, Supplier<Map<Long, Map<String, Object>>> snapshotSupplier,
                      long snapshotSeq) {
    Batch batch = batches.computeIfAbsent(batchId, Batch::new);
    synchronized (batch) {
      batch.state = BatchState.REBUILDING;
    }
    try {
      Map<Long, Map<String, Object>> snapshot = snapshotSupplier.get();
      rebuildBarrier.run();
      synchronized (batch) {
        Map<Long, Map<String, Object>> merged = new HashMap<>(snapshot);
        for (ChangeEvent event : batch.deferred) {
          applyInto(merged, event);
        }
        batch.deferred.clear();
        batch.data = Map.copyOf(merged);
        batch.appliedSeq = Math.max(batch.appliedSeq, snapshotSeq);
        batch.version++;
        batch.state = BatchState.ACTIVE;
      }
      stats.recordRebuild();
    } catch (RuntimeException e) {
      synchronized (batch) {
        batch.state = BatchState.ACTIVE;
      }
      throw e;
    }
  }

  void setRebuildBarrier(Runnable barrier) {
    this.rebuildBarrier = barrier == null ? () -> { } : barrier;
  }

  private static Map<Long, Map<String, Object>> applyTo(Map<Long, Map<String, Object>> data,
                                                        ChangeEvent event) {
    Map<Long, Map<String, Object>> next = new HashMap<>(data);
    applyInto(next, event);
    return Map.copyOf(next);
  }

  private static void applyInto(Map<Long, Map<String, Object>> data, ChangeEvent event) {
    if (event.op() == OperationType.UPSERT) {
      data.put(event.pk(), event.fields());
    } else {
      data.remove(event.pk());
    }
  }

  // ---- 测试辅助：模拟列存数据损坏 ----

  void debugPut(long pk, Map<String, Object> fields) {
    Batch batch = batches.computeIfAbsent(batchOf(pk), Batch::new);
    synchronized (batch) {
      Map<Long, Map<String, Object>> next = new HashMap<>(batch.data);
      next.put(pk, Map.copyOf(fields));
      batch.data = Map.copyOf(next);
    }
  }

  void debugRemove(long pk) {
    Batch batch = batches.get(batchOf(pk));
    if (batch == null) {
      return;
    }
    synchronized (batch) {
      Map<Long, Map<String, Object>> next = new HashMap<>(batch.data);
      next.remove(pk);
      batch.data = Map.copyOf(next);
    }
  }
}
