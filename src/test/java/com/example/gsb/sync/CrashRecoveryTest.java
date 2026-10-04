package com.example.gsb.sync;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 崩溃恢复：队列未消费完时重启，必须补齐且不重复应用。 */
class CrashRecoveryTest {

    @TempDir
    Path dir;

    private DualStore.Config config() {
        return new DualStore.Config(10, 0, 60_000);
    }

    @Test
    void unconsumedEventsAreReplayedAfterRestart() {
        // 阶段一：暂停引擎写入（列存一条都没收到），随后"崩溃"关闭
        try (DualStore store = new DualStore(dir, config())) {
            store.pauseSync();
            for (long k = 1; k <= 5; k++) {
                store.upsert(k, Map.of("v", "v" + k));
            }
            assertThat(store.columnGet(1).row()).isEmpty();
        }

        // 阶段二：重启——行存由 WAL 重建，引擎重放 ack 之后的事件补齐列存
        try (DualStore store = new DualStore(dir, config())) {
            for (long k = 1; k <= 5; k++) {
                assertThat(store.rowGet(k)).as("row %d rebuilt from WAL", k)
                        .contains(Map.of("v", "v" + k));
            }
            assertThat(store.awaitSynced(5_000)).isTrue();
            for (long k = 1; k <= 5; k++) {
                assertThat(store.columnGet(k).row()).contains(Map.of("v", "v" + k));
            }
            assertThat(store.metrics().appliedCount()).isEqualTo(5);
            assertThat(store.metrics().duplicateCount()).isZero();
        }

        // 阶段三：再次重启，没有新事件——位点已持久化，列存快照已落盘，无任何重复应用
        try (DualStore store = new DualStore(dir, config())) {
            assertThat(store.awaitSynced(5_000)).isTrue();
            assertThat(store.metrics().appliedCount()).isZero();
            assertThat(store.columnGet(3).row()).contains(Map.of("v", "v3"));
        }
    }

    @Test
    void applyingSameEventTwiceIsIdempotent() {
        ColumnStore columnStore = new ColumnStore(dir, 10);
        ChangeEvent first = ChangeEvent.upsert(10, 1, Map.of("v", "a"));
        first = new ChangeEvent(10, 1, ChangeEvent.Type.UPSERT, first.timestampMillis,
                first.fields);
        ChangeEvent duplicate = new ChangeEvent(10, 1, ChangeEvent.Type.UPSERT,
                first.timestampMillis + 1, Map.of("v", "a"));

        assertThat(columnStore.apply(first)).isTrue();
        assertThat(columnStore.apply(duplicate)).isFalse();
        assertThat(columnStore.apply(first)).isFalse(); // 重放旧事件也不生效
        assertThat(columnStore.query(1).row()).contains(Map.of("v", "a"));

        // 更新事件 seq 更大，正常生效
        ChangeEvent newer = ChangeEvent.upsert(11, 1, Map.of("v", "b"));
        assertThat(columnStore.apply(newer)).isTrue();
        assertThat(columnStore.query(1).row()).contains(Map.of("v", "b"));
    }

    @Test
    void applyThenAckLostOnCrashDoesNotDuplicate() {
        // 模拟"已应用但 ack 未持久化"：重启后列存从磁盘快照加载，事件重放时按 seq 去重
        try (DualStore store = new DualStore(dir, config())) {
            store.upsert(42, Map.of("v", "x"));
            assertThat(store.awaitSynced(5_000)).isTrue();
        }
        ColumnStore columnStore = new ColumnStore(dir, 10);
        ChangeEvent replayed = new ChangeEvent(1, 42, ChangeEvent.Type.UPSERT,
                System.currentTimeMillis(), Map.of("v", "x"));
        assertThat(columnStore.query(42).row()).contains(Map.of("v", "x"));
        assertThat(columnStore.apply(replayed)).isFalse();
    }
}
