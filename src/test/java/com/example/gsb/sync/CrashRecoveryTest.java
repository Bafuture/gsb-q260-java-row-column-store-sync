package com.example.gsb.sync;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 同步队列未消费完时崩溃，重启后必须补齐且不重复应用。 */
class CrashRecoveryTest {

  @TempDir
  Path dir;

  @Test
  void pendingEventsAreReplayedAfterRestart() {
    DualStore crashed = new DualStore(dir, 10, false, 5);
    crashed.insert(1, Map.of("v", "a"));
    crashed.insert(2, Map.of("v", "b"));
    crashed.update(1, Map.of("v", "a2"));
    crashed.close();

    try (DualStore recovered = new DualStore(dir, 10, false, 5)) {
      assertThat(recovered.pendingSyncCount()).isEqualTo(3);
      recovered.flush();
      assertThat(recovered.getColumn(1)).hasValueSatisfying(
          row -> assertThat(row).containsEntry("v", "a2"));
      assertThat(recovered.getColumn(2)).hasValueSatisfying(
          row -> assertThat(row).containsEntry("v", "b"));
      assertThat(recovered.pendingSyncCount()).isZero();
    }

    try (DualStore again = new DualStore(dir, 10, false, 5)) {
      assertThat(again.pendingSyncCount()).as("ack 已持久化，不再重投").isZero();
    }
  }

  @Test
  void redeliveredEventsAreAppliedExactlyOnce() throws Exception {
    FileSyncQueue queue = new FileSyncQueue(dir);
    RowStore rowStore = new RowStore(queue);
    rowStore.insert(1, Map.of("v", "a"));
    rowStore.insert(2, Map.of("v", "b"));

    ColumnStore columnStore = new ColumnStore(10, new SyncStats());
    ChangeEvent first = queue.poll();
    assertThat(columnStore.apply(first)).isTrue();
    queue.close();

    FileSyncQueue reopened = new FileSyncQueue(dir);
    assertThat(reopened.pendingCount()).isEqualTo(2);

    ChangeEvent replayed = reopened.poll();
    assertThat(replayed.seq()).isEqualTo(first.seq());
    assertThat(columnStore.apply(replayed)).as("同一 seq 重投被幂等跳过").isFalse();

    ChangeEvent second = reopened.poll();
    assertThat(columnStore.apply(second)).isTrue();
    reopened.ack(second.seq());
    reopened.close();

    assertThat(columnStore.get(1)).hasValueSatisfying(
        row -> assertThat(row).containsEntry("v", "a"));
    assertThat(columnStore.get(2)).hasValueSatisfying(
        row -> assertThat(row).containsEntry("v", "b"));

    FileSyncQueue third = new FileSyncQueue(dir);
    assertThat(third.pendingCount()).isZero();
    third.close();
  }
}
