package com.example.gsb.sync;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 列存批次损坏时只重建该批次，且重建期间查询仍可用。 */
class BatchRebuildTest {

  @TempDir
  Path dir;

  @Test
  void rebuildFixesOnlyTheCorruptedBatch() {
    try (DualStore store = new DualStore(dir, 10, false, 5)) {
      store.insert(1, Map.of("v", "batch0-a"));
      store.insert(2, Map.of("v", "batch0-b"));
      store.insert(25, Map.of("v", "batch2"));
      store.flush();

      int batch2VersionBefore = store.getBatchStatus(25).version();

      store.columnStore().debugPut(1, Map.of("v", "CORRUPT"));
      store.columnStore().debugPut(5, Map.of("v", "ghost"));
      assertThat(store.checkConsistency(0, 100)).isNotEmpty();

      long targetBatch = store.batchIdOf(1);
      assertThat(targetBatch).isZero();
      store.rebuildBatch(targetBatch);

      assertThat(store.getColumn(1)).hasValueSatisfying(
          row -> assertThat(row).containsEntry("v", "batch0-a"));
      assertThat(store.getColumn(2)).hasValueSatisfying(
          row -> assertThat(row).containsEntry("v", "batch0-b"));
      assertThat(store.getColumn(5)).isEmpty();
      assertThat(store.getColumn(25)).hasValueSatisfying(
          row -> assertThat(row).containsEntry("v", "batch2"));
      assertThat(store.getBatchStatus(25).version()).isEqualTo(batch2VersionBefore);
      assertThat(store.checkConsistency(0, 100)).isEmpty();
    }
  }

  @Test
  void queriesKeepReturningOldBatchDuringRebuild() throws Exception {
    try (DualStore store = new DualStore(dir, 10, false, 5)) {
      store.insert(1, Map.of("v", "good"));
      store.flush();
      store.columnStore().debugPut(1, Map.of("v", "corrupt-old"));

      CountDownLatch rebuildEntered = new CountDownLatch(1);
      CountDownLatch releaseRebuild = new CountDownLatch(1);
      store.columnStore().setRebuildBarrier(() -> {
        rebuildEntered.countDown();
        try {
          assertThat(releaseRebuild.await(5, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      Thread rebuild = new Thread(() -> store.rebuildBatch(0), "rebuild");
      rebuild.start();
      assertThat(rebuildEntered.await(5, TimeUnit.SECONDS)).isTrue();

      ColumnStore.BatchStatus status = store.getBatchStatus(1);
      assertThat(status.state()).isEqualTo(ColumnStore.BatchState.REBUILDING);
      assertThat(store.getColumn(1)).as("重建期间仍返回旧批次数据")
          .hasValueSatisfying(row -> assertThat(row).containsEntry("v", "corrupt-old"));

      releaseRebuild.countDown();
      rebuild.join(5_000);

      assertThat(store.getBatchStatus(1).state()).isEqualTo(ColumnStore.BatchState.ACTIVE);
      assertThat(store.getColumn(1)).hasValueSatisfying(
          row -> assertThat(row).containsEntry("v", "good"));
    }
  }

  @Test
  void updatesDuringRebuildAreNotLost() throws Exception {
    try (DualStore store = new DualStore(dir, 10, false, 5)) {
      store.insert(1, Map.of("v", "v1"));
      store.flush();

      CountDownLatch rebuildEntered = new CountDownLatch(1);
      CountDownLatch releaseRebuild = new CountDownLatch(1);
      store.columnStore().setRebuildBarrier(() -> {
        rebuildEntered.countDown();
        try {
          releaseRebuild.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      Thread rebuild = new Thread(() -> store.rebuildBatch(0), "rebuild");
      rebuild.start();
      assertThat(rebuildEntered.await(5, TimeUnit.SECONDS)).isTrue();

      store.update(1, Map.of("v", "v2"));
      store.flush();

      releaseRebuild.countDown();
      rebuild.join(5_000);

      assertThat(store.getColumn(1)).hasValueSatisfying(
          row -> assertThat(row).containsEntry("v", "v2"));
      assertThat(store.checkConsistency(0, 100)).isEmpty();
    }
  }
}
