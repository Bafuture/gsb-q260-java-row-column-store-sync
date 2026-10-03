package com.example.gsb.sync;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 行存插入/更新/删除会异步触发同步到列存。 */
class SyncPropagationTest {

  @TempDir
  Path dir;

  @Test
  void insertUpdateDeleteArePropagatedToColumnStore() {
    try (DualStore store = new DualStore(dir, 10, false, 5)) {
      store.insert(1, Map.of("name", "alice", "age", 30));
      store.flush();
      assertThat(store.getColumn(1)).hasValueSatisfying(
          row -> assertThat(row).containsEntry("name", "alice").containsEntry("age", 30));

      store.update(1, Map.of("age", 31));
      store.flush();
      assertThat(store.getColumn(1)).hasValueSatisfying(
          row -> assertThat(row).containsEntry("name", "alice").containsEntry("age", 31));

      store.delete(1);
      store.flush();
      assertThat(store.getColumn(1)).isEmpty();
    }
  }

  @Test
  void backgroundWorkerSyncsAsynchronously() {
    try (DualStore store = new DualStore(dir, 10, true, 2)) {
      store.insert(7, Map.of("v", "x"));
      assertThat(store.awaitIdle(5_000)).isTrue();
      assertThat(store.getColumn(7)).hasValueSatisfying(
          row -> assertThat(row).containsEntry("v", "x"));
    }
  }

  @Test
  void rowStoreRejectsDuplicateInsertAndUnknownUpdate() {
    try (DualStore store = new DualStore(dir, 10, false, 5)) {
      store.insert(1, Map.of("v", 1));
      org.assertj.core.api.Assertions.assertThatThrownBy(() -> store.insert(1, Map.of("v", 2)))
          .isInstanceOf(IllegalArgumentException.class);
      org.assertj.core.api.Assertions.assertThatThrownBy(() -> store.update(99, Map.of("v", 1)))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }
}
