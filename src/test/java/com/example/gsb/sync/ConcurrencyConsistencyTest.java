package com.example.gsb.sync;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 行存更新与列存重建/同步并发时，正常延迟不得被误判为不一致。 */
class ConcurrencyConsistencyTest {

  @TempDir
  Path dir;

  @Test
  void inFlightChangeIsNotReportedAsMismatch() {
    try (DualStore store = new DualStore(dir, 10, false, 5)) {
      store.insert(1, Map.of("v", "old"));
      store.flush();

      store.update(1, Map.of("v", "new"));
      assertThat(store.checkConsistency(0, 100))
          .as("变更仍在传播窗口内（行 seq > 批次水位），不得误判")
          .isEmpty();

      store.flush();
      assertThat(store.checkConsistency(0, 100)).isEmpty();
    }
  }

  @Test
  void inFlightDeleteIsNotReportedAsMismatch() {
    try (DualStore store = new DualStore(dir, 10, false, 5)) {
      store.insert(1, Map.of("v", "x"));
      store.flush();

      store.delete(1);
      assertThat(store.checkConsistency(0, 100))
          .as("删除事件尚未应用，列存残留行不得误判")
          .isEmpty();

      store.flush();
      assertThat(store.checkConsistency(0, 100)).isEmpty();
    }
  }

  @Test
  void rebuildingBatchIsSkippedByChecker() throws Exception {
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
      assertThat(store.checkConsistency(0, 100))
          .as("重建中的批次整体跳过，不误判")
          .isEmpty();

      releaseRebuild.countDown();
      rebuild.join(5_000);
      store.flush();
      assertThat(store.checkConsistency(0, 100)).isEmpty();
    }
  }

  @Test
  void concurrentUpdatesAndChecksNeverProduceFalsePositives() throws Exception {
    try (DualStore store = new DualStore(dir, 10, true, 1)) {
      for (long pk = 0; pk < 20; pk++) {
        store.insert(pk, Map.of("v", 0));
      }
      store.awaitIdle(5_000);

      ConcurrentLinkedQueue<List<Mismatch>> falsePositives = new ConcurrentLinkedQueue<>();
      Thread updater = new Thread(() -> {
        for (int i = 0; i < 400; i++) {
          long pk = i % 20;
          if (i % 11 == 0 && store.getRow(pk).isPresent()) {
            store.delete(pk);
          } else if (store.getRow(pk).isPresent()) {
            store.update(pk, Map.of("v", i));
          } else {
            store.insert(pk, Map.of("v", i));
          }
        }
      }, "updater");
      updater.start();

      for (int i = 0; i < 150; i++) {
        List<Mismatch> mismatches = store.checkConsistency(0, 100);
        if (!mismatches.isEmpty()) {
          falsePositives.add(mismatches);
        }
      }
      updater.join(10_000);

      assertThat(falsePositives).as("并发窗口内的正常延迟不得被误判").isEmpty();

      assertThat(store.awaitIdle(5_000)).isTrue();
      assertThat(store.checkConsistency(0, 100)).as("同步追平后必须完全一致").isEmpty();
    }
  }
}
