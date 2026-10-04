package com.example.gsb.sync;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 一致性检查：输出不一致主键与字段；正常同步延迟不误判。 */
class ConsistencyCheckTest {

    @TempDir
    Path dir;

    private DualStore.Config config() {
        return new DualStore.Config(10, 0, 60_000);
    }

    @Test
    void cleanStoresReportNoInconsistency() {
        try (DualStore store = new DualStore(dir, config())) {
            for (long k = 0; k < 5; k++) {
                store.upsert(k, Map.of("name", "u" + k));
            }
            assertThat(store.awaitSynced(5_000)).isTrue();

            var report = store.checkConsistency(0, 100);
            assertThat(report.isConsistent()).isTrue();
            assertThat(report.compared()).isEqualTo(5);
        }
    }

    @Test
    void corruptedFieldIsReportedWithKeyAndField() {
        try (DualStore store = new DualStore(dir, config())) {
            store.upsert(1, Map.of("name", "alice", "age", "30"));
            store.upsert(2, Map.of("name", "bob"));
            assertThat(store.awaitSynced(5_000)).isTrue();

            store.corruptColumnForTest(2, "name", "CORRUPTED");

            var report = store.checkConsistency(0, 100);
            assertThat(report.inconsistencies()).hasSize(1);
            var inc = report.inconsistencies().get(0);
            assertThat(inc.key()).isEqualTo(2);
            assertThat(inc.kind()).isEqualTo(ConsistencyChecker.Kind.FIELD_MISMATCH);
            assertThat(inc.fieldDiffs()).containsKey("name");
            assertThat(inc.fieldDiffs().get("name")).containsExactly("bob", "CORRUPTED");
        }
    }

    @Test
    void missingOnEitherSideIsReported() {
        try (DualStore store = new DualStore(dir, config())) {
            store.upsert(1, Map.of("name", "alice"));
            assertThat(store.awaitSynced(5_000)).isTrue();

            store.removeColumnEntryForTest(1);                 // 列存丢了
            store.insertColumnEntryForTest(9, Map.of("x", "y"), 999); // 列存多了

            var report = store.checkConsistency(0, 100);
            assertThat(report.inconsistencies()).hasSize(2);
            assertThat(report.inconsistencies())
                    .anySatisfy(i -> {
                        assertThat(i.key()).isEqualTo(1);
                        assertThat(i.kind()).isEqualTo(ConsistencyChecker.Kind.MISSING_IN_COLUMN);
                    })
                    .anySatisfy(i -> {
                        assertThat(i.key()).isEqualTo(9);
                        assertThat(i.kind()).isEqualTo(ConsistencyChecker.Kind.MISSING_IN_ROW);
                    });
        }
    }

    @Test
    void inFlightChangesAreSkippedNotFlagged() {
        try (DualStore store = new DualStore(dir, config())) {
            store.upsert(1, Map.of("v", "old"));
            assertThat(store.awaitSynced(5_000)).isTrue();

            store.pauseSync();
            store.upsert(1, Map.of("v", "new")); // 事件积压，列存还是旧值
            store.upsert(2, Map.of("v", "fresh"));

            var report = store.checkConsistency(0, 100);
            assertThat(report.isConsistent()).isTrue();   // 不误判
            assertThat(report.skippedInFlight()).isEqualTo(2);

            store.resumeSync();
            assertThat(store.awaitSynced(5_000)).isTrue();
            assertThat(store.checkConsistency(0, 100).isConsistent()).isTrue();
        }
    }

    @Test
    void recentChangesWithinLagToleranceAreSkipped() {
        // 判定窗口：变更时间距今小于 lagTolerance 的主键跳过
        try (DualStore store = new DualStore(dir, new DualStore.Config(10, 60_000, 60_000))) {
            store.upsert(1, Map.of("v", "1"));
            assertThat(store.awaitSynced(5_000)).isTrue();
            store.corruptColumnForTest(1, "v", "BROKEN");

            var report = store.checkConsistency(0, 100);
            assertThat(report.isConsistent()).isTrue(); // 在判定窗口内，跳过
            assertThat(report.skippedInFlight()).isEqualTo(1);
        }
    }
}
