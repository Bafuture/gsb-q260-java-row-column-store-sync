package com.example.gsb.sync;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 并发：行存更新与列存重建同时进行时，一致性检查不得把正常延迟误判为不一致。 */
class ConcurrencyJudgmentWindowTest {

    @TempDir
    Path dir;

    @Test
    void updatesDuringRebuildAreNotFlaggedAsInconsistent() throws Exception {
        try (DualStore store = new DualStore(dir, new DualStore.Config(10, 0, 60_000))) {
            for (long k = 0; k < 20; k++) {
                store.upsert(k, Map.of("v", "v" + k));
            }
            assertThat(store.awaitSynced(5_000)).isTrue();

            CountDownLatch rebuildEntered = new CountDownLatch(1);
            CountDownLatch releaseRebuild = new CountDownLatch(1);
            store.setRebuildHook(() -> {
                rebuildEntered.countDown();
                try {
                    releaseRebuild.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            Thread rebuilder = new Thread(() -> store.rebuildBatch(0));
            rebuilder.start();
            assertThat(rebuildEntered.await(5, TimeUnit.SECONDS)).isTrue();

            try {
                // 重建进行中：行存继续点更新（批次 0 内 + 其他批次）
                store.upsert(2, Map.of("v", "updated-2"));
                store.upsert(15, Map.of("v", "updated-15"));
                store.delete(3);

                // 判定窗口：重建中的批次整体跳过；在途事件（seq > ack）跳过
                var report = store.checkConsistency(0, 19);
                assertThat(report.isConsistent())
                        .as("重建与在途同步属正常延迟，不得误判")
                        .isTrue();
                assertThat(report.skippedInFlight()).isGreaterThan(0);
            } finally {
                releaseRebuild.countDown();
            }
            rebuilder.join(5_000);
            assertThat(rebuilder.isAlive()).isFalse();

            // 重建与同步都结束后：并发更新已正确合入新批次，最终一致
            assertThat(store.awaitSynced(5_000)).isTrue();
            assertThat(store.columnGet(2).row()).contains(Map.of("v", "updated-2"));
            assertThat(store.columnGet(15).row()).contains(Map.of("v", "updated-15"));
            assertThat(store.columnGet(3).row()).isEmpty();
            assertThat(store.checkConsistency(0, 19).isConsistent()).isTrue();
        }
    }

    @Test
    void sustainedWritesDuringRebuildConvergeWithoutFalsePositives() throws Exception {
        try (DualStore store = new DualStore(dir, new DualStore.Config(10, 0, 60_000))) {
            for (long k = 0; k < 50; k++) {
                store.upsert(k, Map.of("v", "init-" + k));
            }
            assertThat(store.awaitSynced(5_000)).isTrue();

            CountDownLatch stopWrites = new CountDownLatch(1);
            Thread writer = new Thread(() -> {
                long round = 0;
                while (stopWrites.getCount() > 0) {
                    for (long k = 0; k < 50 && stopWrites.getCount() > 0; k++) {
                        store.upsert(k, Map.of("v", "r" + round + "-" + k));
                    }
                    round++;
                }
            });
            writer.start();

            // 写压持续期间重建全部批次并反复检查，均不得出现误判
            for (long batch = 0; batch < 5; batch++) {
                store.rebuildBatch(batch);
                var report = store.checkConsistency(0, 49);
                assertThat(report.isConsistent())
                        .as("batch %d 重建期间不得误判", batch)
                        .isTrue();
            }

            stopWrites.countDown();
            writer.join(5_000);
            assertThat(store.awaitSynced(10_000)).isTrue();
            assertThat(store.checkConsistency(0, 49).isConsistent()).isTrue();
        }
    }
}
