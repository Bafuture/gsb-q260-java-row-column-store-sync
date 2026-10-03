package com.example.gsb.sync;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 一致性检查：输出不一致的主键与字段。 */
class ConsistencyCheckTest {

  @TempDir
  Path dir;

  private DualStore openAndSeed() {
    DualStore store = new DualStore(dir, 10, false, 5);
    store.insert(1, Map.of("name", "alice", "age", 30));
    store.insert(2, Map.of("name", "bob", "age", 40));
    store.flush();
    return store;
  }

  @Test
  void consistentStoresReportNoMismatch() {
    try (DualStore store = openAndSeed()) {
      assertThat(store.checkConsistency(0, 100)).isEmpty();
    }
  }

  @Test
  void fieldDiffIsReportedWithPkAndField() {
    try (DualStore store = openAndSeed()) {
      store.columnStore().debugPut(1, Map.of("name", "alice", "age", 999));
      List<Mismatch> mismatches = store.checkConsistency(0, 100);
      assertThat(mismatches).containsExactly(
          new Mismatch(1, "age", 30, 999, Mismatch.Type.FIELD_DIFF));
    }
  }

  @Test
  void missingColumnRowIsReported() {
    try (DualStore store = openAndSeed()) {
      store.columnStore().debugRemove(2);
      List<Mismatch> mismatches = store.checkConsistency(0, 100);
      assertThat(mismatches).singleElement().satisfies(m -> {
        assertThat(m.pk()).isEqualTo(2);
        assertThat(m.type()).isEqualTo(Mismatch.Type.ONLY_IN_ROW);
      });
    }
  }

  @Test
  void ghostColumnRowIsReported() {
    try (DualStore store = openAndSeed()) {
      store.columnStore().debugPut(5, Map.of("name", "ghost"));
      List<Mismatch> mismatches = store.checkConsistency(0, 100);
      assertThat(mismatches).singleElement().satisfies(m -> {
        assertThat(m.pk()).isEqualTo(5);
        assertThat(m.type()).isEqualTo(Mismatch.Type.ONLY_IN_COLUMN);
      });
    }
  }

  @Test
  void checkIsScopedToGivenRange() {
    try (DualStore store = openAndSeed()) {
      store.columnStore().debugPut(1, Map.of("name", "alice", "age", 999));
      assertThat(store.checkConsistency(50, 100)).isEmpty();
      assertThat(store.checkConsistency(0, 10)).isNotEmpty();
    }
  }
}
