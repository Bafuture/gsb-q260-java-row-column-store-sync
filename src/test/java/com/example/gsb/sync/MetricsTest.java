package com.example.gsb.sync;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 统计：同步条数、延迟分布、不一致条数、重建次数。 */
class MetricsTest {

    @TempDir
    Path dir;

    @Test
    void metricsTrackSyncLatencyInconsistencyAndRebuild() {
        try (DualStore store = new DualStore(dir, new DualStore.Config(10, 0, 60_000))) {
            store.upsert(1, Map.of("v", "a"));
            store.upsert(2, Map.of("v", "b"));
            store.upsert(3, Map.of("v", "c"));
            store.upsert(1, Map.of("v", "a2")); // 更新
            store.delete(3);                     // 删除
            assertThat(store.awaitSynced(5_000)).isTrue();

            SyncMetrics metrics = store.metrics();
            assertThat(metrics.appliedCount()).isEqualTo(5);
            assertThat(metrics.duplicateCount()).isZero();

            // 延迟分布：所有桶之和等于同步条数
            Map<String, Long> dist = metrics.latencyDistribution();
            assertThat(dist).isNotEmpty();
            assertThat(dist.values().stream().mapToLong(Long::longValue).sum()).isEqualTo(5);

            // 不一致条数
            store.corruptColumnForTest(2, "v", "BROKEN");
            var report = store.checkConsistency(0, 100);
            assertThat(report.inconsistencies()).hasSize(1);
            assertThat(metrics.inconsistencyCount()).isEqualTo(1);

            // 重建次数
            store.rebuildBatch(0);
            assertThat(metrics.rebuildCount()).isEqualTo(1);
            assertThat(store.checkConsistency(0, 100).isConsistent()).isTrue();
        }
    }
}
