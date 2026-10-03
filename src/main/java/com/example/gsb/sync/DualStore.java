package com.example.gsb.sync;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 行列双存门面：组装行存、文件队列、列存、异步同步线程与一致性检查。
 *
 * <p>用法：
 * <pre>{@code
 * try (DualStore store = DualStore.open(queueDir, 1000)) {
 *   store.insert(1, Map.of("name", "a"));   // 异步同步
 *   store.flush();                          // 或等待追平
 *   store.checkConsistency(0, 10_000);
 *   store.rebuildBatch(0);                  // 只重建损坏批次
 * }
 * }</pre>
 *
 * <p>崩溃恢复：重启时用同一 queueDir 打开，构造期间自动回放未 ack 的事件，
 * 由异步线程（或手动 {@link #flush()}）补齐，列存按 seq 去重不重复应用。
 */
public class DualStore implements AutoCloseable {

  private final long batchSize;
  private final FileSyncQueue queue;
  private final RowStore rowStore;
  private final ColumnStore columnStore;
  private final SyncStats stats;
  private final SyncWorker worker;
  private final ConsistencyChecker checker;

  public DualStore(Path queueDirectory, long batchSize) {
    this(queueDirectory, batchSize, true, 5);
  }

  public DualStore(Path queueDirectory, long batchSize, boolean startWorker,
                   long pollIntervalMillis) {
    this.batchSize = batchSize;
    try {
      this.queue = new FileSyncQueue(queueDirectory);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    this.stats = new SyncStats();
    this.rowStore = new RowStore(queue);
    this.columnStore = new ColumnStore(batchSize, stats);
    this.checker = new ConsistencyChecker(rowStore, columnStore, stats);
    this.worker = new SyncWorker(queue, columnStore, stats, pollIntervalMillis);
    if (startWorker) {
      worker.start();
    }
  }

  public long insert(long pk, Map<String, Object> fields) {
    return rowStore.insert(pk, fields);
  }

  public long update(long pk, Map<String, Object> fields) {
    return rowStore.update(pk, fields);
  }

  public long delete(long pk) {
    return rowStore.delete(pk);
  }

  public Optional<RowStore.Row> getRow(long pk) {
    return rowStore.get(pk);
  }

  /** 查列存；重建中的批次仍返回旧批次数据。 */
  public Optional<Map<String, Object>> getColumn(long pk) {
    return columnStore.get(pk);
  }

  public ColumnStore.BatchStatus getBatchStatus(long pk) {
    return columnStore.status(pk);
  }

  /** 比对范围内行列数据，返回不一致的主键与字段。 */
  public List<Mismatch> checkConsistency(long fromPkInclusive, long toPkInclusive) {
    return checker.check(fromPkInclusive, toPkInclusive);
  }

  /**
   * 只重建指定批次：从行存取该批次主键范围的快照，重建期间查询继续返回旧批次。
   */
  public void rebuildBatch(long batchId) {
    long[] range = columnStore.batchRange(batchId);
    long snapshotSeq = rowStore.currentSeq();
    columnStore.rebuild(batchId, () -> rowStore.snapshotRange(range[0], range[1]), snapshotSeq);
  }

  /** 阻塞消费完队列积压（主要用于确定性测试和需要强一致的读路径）。 */
  public int flush() {
    return worker.drain();
  }

  /** 等待异步同步追平。 */
  public boolean awaitIdle(long timeoutMillis) {
    return worker.awaitIdle(timeoutMillis);
  }

  /** 重启后未消费完的事件条数。 */
  public int pendingSyncCount() {
    return queue.pendingCount();
  }

  public long batchIdOf(long pk) {
    return columnStore.batchOf(pk);
  }

  public SyncStats.Snapshot stats() {
    return stats.snapshot();
  }

  public RowStore rowStore() {
    return rowStore;
  }

  public ColumnStore columnStore() {
    return columnStore;
  }

  public SyncQueue queue() {
    return queue;
  }

  @Override
  public void close() {
    worker.close();
    queue.close();
  }
}
