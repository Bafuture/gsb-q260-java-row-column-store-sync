package com.example.gsb.sync;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 行式存储：按主键点查点改，变更先落本地再投递同步队列。
 *
 * <p>每次变更都带上一个全局单调递增的 {@code seq}：行存记录每个主键的最后变更 seq，
 * 一致性检查据此判断某行是否仍处于异步传播窗口内。
 */
public class RowStore {

  /** 一行数据。 */
  public record Row(long pk, Map<String, Object> fields, long lastSeq, long lastModifiedMillis) {
    public Row {
      fields = fields == null ? Map.of() : Map.copyOf(fields);
    }
  }

  private final ConcurrentHashMap<Long, Row> rows = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Long, Long> lastChangeSeq = new ConcurrentHashMap<>();
  private final AtomicLong seqGen = new AtomicLong(0L);
  private final SyncQueue queue;
  private final Object mutateLock = new Object();

  public RowStore(SyncQueue queue) {
    this.queue = Objects.requireNonNull(queue);
  }

  /** 插入新主键。 */
  public long insert(long pk, Map<String, Object> fields) {
    Objects.requireNonNull(fields, "fields");
    synchronized (mutateLock) {
      if (rows.containsKey(pk)) {
        throw new IllegalArgumentException("主键已存在: " + pk);
      }
      long seq = seqGen.incrementAndGet();
      long now = System.currentTimeMillis();
      Row row = new Row(pk, Map.copyOf(fields), seq, now);
      rows.put(pk, row);
      lastChangeSeq.put(pk, seq);
      queue.append(new ChangeEvent(seq, pk, OperationType.UPSERT, row.fields(), now));
      return seq;
    }
  }

  /** 更新已有主键（字段合并）。事件携带合并后的整行镜像。 */
  public long update(long pk, Map<String, Object> fields) {
    Objects.requireNonNull(fields, "fields");
    synchronized (mutateLock) {
      Row existing = rows.get(pk);
      if (existing == null) {
        throw new IllegalArgumentException("主键不存在: " + pk);
      }
      Map<String, Object> merged = new HashMap<>(existing.fields());
      merged.putAll(fields);
      long seq = seqGen.incrementAndGet();
      long now = System.currentTimeMillis();
      Row row = new Row(pk, merged, seq, now);
      rows.put(pk, row);
      lastChangeSeq.put(pk, seq);
      queue.append(new ChangeEvent(seq, pk, OperationType.UPSERT, row.fields(), now));
      return seq;
    }
  }

  /** 按主键删除。 */
  public long delete(long pk) {
    synchronized (mutateLock) {
      long seq = seqGen.incrementAndGet();
      long now = System.currentTimeMillis();
      rows.remove(pk);
      lastChangeSeq.put(pk, seq);
      queue.append(new ChangeEvent(seq, pk, OperationType.DELETE, Map.of(), now));
      return seq;
    }
  }

  public Optional<Row> get(long pk) {
    return Optional.ofNullable(rows.get(pk));
  }

  public boolean contains(long pk) {
    return rows.containsKey(pk);
  }

  /** 主键最后一次变更的 seq（含已删除主键的删除 seq），从未变更返回 0。 */
  public long lastChangeSeq(long pk) {
    return lastChangeSeq.getOrDefault(pk, 0L);
  }

  public long currentSeq() {
    return seqGen.get();
  }

  /** 范围内现存行的快照（用于重建与一致性检查）。 */
  public List<Row> rowsInRange(long fromPkInclusive, long toPkInclusive) {
    List<Row> snapshot = new ArrayList<>();
    for (Row row : rows.values()) {
      if (row.pk() >= fromPkInclusive && row.pk() <= toPkInclusive) {
        snapshot.add(row);
      }
    }
    return snapshot;
  }

  /** 范围内主键到整行镜像的快照，列存批次重建时使用。 */
  public Map<Long, Map<String, Object>> snapshotRange(long fromPkInclusive, long toPkInclusive) {
    Map<Long, Map<String, Object>> snapshot = new HashMap<>();
    for (Row row : rows.values()) {
      if (row.pk() >= fromPkInclusive && row.pk() <= toPkInclusive) {
        snapshot.put(row.pk(), row.fields());
      }
    }
    return snapshot;
  }
}
