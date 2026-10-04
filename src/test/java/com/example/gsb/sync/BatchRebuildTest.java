package com.example.gsb.sync;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 列存批次重建：只重建损坏批次；重建期间查询返回旧批次数据并标记重建中。 */
class BatchRebuildTest {

    @TempDir
    Path dir;

    private DualStore.Config config() {
        return new DualStore.Config(10, 0, 60_000);
    }

    @Test
    void onlyCorruptedBatchIsRebuilt() {
        try (DualStore store = new DualStore(dir, config())) {
            for (long k = 0; k < 20; k++) {
                store.upsert(k, Map.of("v", "v" + k));
            }
            assertThat(store.awaitSynced(5_000)).isTrue();

            assertThat(store.batchIdOf(3)).isEqualTo(0);
            assertThat(store.batchIdOf(13)).isEqualTo(1);
            store.corruptColumnForTest(3, "v", "BROKEN-0");
            store.corruptColumnForTest(13, "v", "BROKEN-1");

            store.rebuildBatch(0); // 只重建批次 0

            assertThat(store.columnGet(3).row()).contains(Map.of("v", "v3"));
            assertThat(store.columnGet(13).row()).contains(Map.of("v", "BROKEN-1"));

            var report = store.checkConsistency(0, 19);
            assertThat(report.inconsistencies()).extracting(ConsistencyChecker.Inconsistency::key)
                    .containsExactly(13L);

            store.rebuildBatch(1);
            assertThat(store.checkConsistency(0, 19).isConsistent()).isTrue();
            assertThat(store.metrics().rebuildCount()).isEqualTo(2);
        }
    }

    @Test
    void queriesRemainAvailableDuringRebuild() throws Exception {
        try (DualStore store = new DualStore(dir, config())) {
            store.upsert(5, Map.of("v", "old"));
            assertThat(store.awaitSynced(5_000)).isTrue();

            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            store.setRebuildHook(() -> {
                entered.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            Thread rebuilder = new Thread(() -> store.rebuildBatch(0));
            rebuilder.start();
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

            try {
                // 重建中：状态为 REBUILDING，但查询仍返回旧批次数据
                assertThat(store.batchStateOf(5)).isEqualTo(ColumnStore.BatchState.REBUILDING);
                var result = store.columnGet(5);
                assertThat(result.batchState()).isEqualTo(ColumnStore.BatchState.REBUILDING);
                assertThat(result.row()).contains(Map.of("v", "old"));
            } finally {
                release.countDown();
            }
            rebuilder.join(5_000);
            assertThat(rebuilder.isAlive()).isFalse();
            assertThat(store.batchStateOf(5)).isEqualTo(ColumnStore.BatchState.ACTIVE);
            assertThat(store.columnGet(5).row()).contains(Map.of("v", "old"));
        }
    }
}
