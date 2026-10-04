package com.example.gsb.sync;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 行存插入/更新/删除后，异步同步最终体现在列存。 */
class RowUpdateTriggersSyncTest {

    @TempDir
    Path dir;

    private DualStore.Config config() {
        return new DualStore.Config(10, 0, 60_000);
    }

    @Test
    void upsertUpdateDeletePropagateToColumnStore() {
        try (DualStore store = new DualStore(dir, config())) {
            store.upsert(1, Map.of("name", "alice", "age", "30"));
            store.upsert(2, Map.of("name", "bob"));
            assertThat(store.awaitSynced(5_000)).isTrue();

            assertThat(store.columnGet(1).row()).contains(Map.of("name", "alice", "age", "30"));
            assertThat(store.columnGet(2).row()).contains(Map.of("name", "bob"));
            assertThat(store.columnGet(1).batchState()).isEqualTo(ColumnStore.BatchState.ACTIVE);

            // 更新
            store.upsert(1, Map.of("name", "alice", "age", "31"));
            assertThat(store.awaitSynced(5_000)).isTrue();
            assertThat(store.columnGet(1).row()).contains(Map.of("name", "alice", "age", "31"));

            // 删除
            store.delete(1);
            assertThat(store.awaitSynced(5_000)).isTrue();
            assertThat(store.columnGet(1).row()).isEmpty();
            assertThat(store.rowGet(1)).isEmpty();
            assertThat(store.rowGet(2)).contains(Map.of("name", "bob"));
        }
    }

    @Test
    void syncIsAsynchronousButEventuallyConsistent() {
        try (DualStore store = new DualStore(dir, config())) {
            store.pauseSync();
            store.upsert(7, Map.of("v", "1"));
            // 行存立即可见，列存尚未同步（异步）
            assertThat(store.rowGet(7)).contains(Map.of("v", "1"));
            assertThat(store.columnGet(7).row()).isEmpty();

            store.resumeSync();
            assertThat(store.awaitSynced(5_000)).isTrue();
            assertThat(store.columnGet(7).row()).contains(Map.of("v", "1"));
        }
    }
}
