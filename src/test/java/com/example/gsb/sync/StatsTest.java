package com.example.gsb.sync;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 统计：同步条数、延迟分布、不一致条数、重建次数。 */
class StatsTest {

  @TempDir
  Path dir;

  @Test
  void statsAreCollected() {
    try (DualStore store = new DualStore(dir, 10, false, 5)) {
      store.insert(1, Map.of("v", 1));
      store.insert(2, Map.of("v", 2));
      store.update(1, Map.of("v", 3));
      store.delete(2);
      store.flush();

      SyncStats.Snapshot stats = store.stats();
      assertThat(stats.syncedCount()).isEqualTo(4);
      assertThat(stats.latencyCount()).isEqualTo(4);
      assertThat(stats.avgLatencyMillis()).isGreaterThanOrEqualTo(0.0);
      assertThat(stats.maxLatencyMillis()).isGreaterThanOrEqualTo(0);
      assertThat(stats.latencyBuckets().values()).allSatisfy(v -> assertThat(v).isGreaterThanOrEqualTo(0));
      assertThat(stats.latencyBuckets().values().stream().mapToLong(Long::longValue).sum())
          .isEqualTo(4);

      store.columnStore().debugPut(1, Map.of("v", 999));
      assertThat(store.checkConsistency(0, 100)).hasSize(1);
      assertThat(store.stats().mismatchCount()).isEqualTo(1);

      store.rebuildBatch(0);
      assertThat(store.stats().rebuildCount()).isEqualTo(1);
      assertThat(store.checkConsistency(0, 100)).isEmpty();
    }
  }

  @Test
  void duplicateRedeliveryIsCounted() throws Exception {
    FileSyncQueue queue = new FileSyncQueue(dir);
    RowStore rowStore = new RowStore(queue);
    rowStore.insert(1, Map.of("v", "a"));
    SyncStats stats = new SyncStats();
    ColumnStore columnStore = new ColumnStore(10, stats);
    SyncWorker worker = new SyncWorker(queue, columnStore, stats, 1);

    assertThat(worker.drain()).isEqualTo(1);
    ChangeEvent replayed = new ChangeEvent(1, 1, OperationType.UPSERT, Map.of("v", "a"),
        System.currentTimeMillis());
    assertThat(columnStore.apply(replayed)).isFalse();
    stats.recordDuplicateSkipped();

    assertThat(stats.snapshot().duplicateSkipped()).isEqualTo(1);
    queue.close();
  }
}
