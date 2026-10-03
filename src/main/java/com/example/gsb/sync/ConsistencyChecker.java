package com.example.gsb.sync;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 按主键范围比对行存与列存。
 *
 * <p><b>判定窗口（避免把正常延迟误判为不一致）：</b>
 * <ul>
 *   <li>行存每行记录最后变更 seq，列存每个批次记录已应用水位 appliedSeq；
 *       若 {@code 行最后seq > 批次appliedSeq}，说明该变更还在异步传播窗口内，跳过不判；</li>
 *   <li>批次处于 REBUILDING 时该批次整体跳过（查询本就返回旧批次）；</li>
 *   <li>并发更新可能让初次读到的瞬时差异失效，因此任何候选不一致都要做一次“稳定确认”：
 *       重读两边、行 seq 与批次水位未变化后仍不一致才计入。</li>
 * </ul>
 */
public class ConsistencyChecker {

  private final RowStore rowStore;
  private final ColumnStore columnStore;
  private final SyncStats stats;

  public ConsistencyChecker(RowStore rowStore, ColumnStore columnStore, SyncStats stats) {
    this.rowStore = rowStore;
    this.columnStore = columnStore;
    this.stats = stats;
  }

  /** 比对 [fromPkInclusive, toPkInclusive] 范围。 */
  public List<Mismatch> check(long fromPkInclusive, long toPkInclusive) {
    List<Mismatch> result = new ArrayList<>();

    for (RowStore.Row row : rowStore.rowsInRange(fromPkInclusive, toPkInclusive)) {
      ColumnStore.Batch batch = columnStore.batch(row.pk());
      if (batch == null || batch.state() == ColumnStore.BatchState.REBUILDING) {
        continue;
      }
      if (row.lastSeq() > batch.appliedSeq()) {
        continue;
      }
      Map<String, Object> columnRow = batch.get(row.pk());
      boolean differs = columnRow == null || !Objects.equals(row.fields(), columnRow);
      if (differs && stableFromRowSide(row.pk(), row.lastSeq())) {
        collectRowSide(row.pk(), result);
      }
    }

    for (ColumnStore.Batch batch : columnStore.batches()) {
      if (batch.state() == ColumnStore.BatchState.REBUILDING) {
        continue;
      }
      for (Map.Entry<Long, Map<String, Object>> entry : batch.data().entrySet()) {
        long pk = entry.getKey();
        if (pk < fromPkInclusive || pk > toPkInclusive) {
          continue;
        }
        if (rowStore.contains(pk)) {
          continue;
        }
        if (rowStore.lastChangeSeq(pk) > batch.appliedSeq()) {
          continue;
        }
        if (stableFromColumnSide(pk, batch.appliedSeq())) {
          result.add(new Mismatch(pk, Mismatch.WHOLE_ROW, null, entry.getValue(),
              Mismatch.Type.ONLY_IN_COLUMN));
        }
      }
    }

    stats.recordMismatches(result.size());
    return result;
  }

  /** 二次确认：行侧仍是同一 seq、批次水位未前进、两边仍然不一致。 */
  private boolean stableFromRowSide(long pk, long capturedRowSeq) {
    RowStore.Row fresh = rowStore.get(pk).orElse(null);
    if (fresh == null || fresh.lastSeq() != capturedRowSeq) {
      return false;
    }
    ColumnStore.Batch batch = columnStore.batch(pk);
    if (batch == null || batch.state() == ColumnStore.BatchState.REBUILDING) {
      return false;
    }
    if (fresh.lastSeq() > batch.appliedSeq()) {
      return false;
    }
    Map<String, Object> columnRow = batch.get(pk);
    return columnRow == null || !Objects.equals(fresh.fields(), columnRow);
  }

  /** 列侧幽灵行的二次确认：行存仍无此主键、水位未前进、列存仍有该行。 */
  private boolean stableFromColumnSide(long pk, long capturedAppliedSeq) {
    if (rowStore.contains(pk)) {
      return false;
    }
    ColumnStore.Batch batch = columnStore.batch(pk);
    if (batch == null || batch.state() == ColumnStore.BatchState.REBUILDING) {
      return false;
    }
    if (batch.appliedSeq() != capturedAppliedSeq) {
      return false;
    }
    if (rowStore.lastChangeSeq(pk) > batch.appliedSeq()) {
      return false;
    }
    return batch.get(pk) != null;
  }

  private void collectRowSide(long pk, List<Mismatch> result) {
    RowStore.Row fresh = rowStore.get(pk).orElseThrow();
    ColumnStore.Batch batch = columnStore.batch(pk);
    Map<String, Object> columnRow = batch.get(pk);
    if (columnRow == null) {
      result.add(new Mismatch(pk, Mismatch.WHOLE_ROW, fresh.fields(), null,
          Mismatch.Type.ONLY_IN_ROW));
      return;
    }
    Set<String> fields = new HashSet<>(fresh.fields().keySet());
    fields.addAll(columnRow.keySet());
    for (String field : fields) {
      Object rowValue = fresh.fields().get(field);
      Object columnValue = columnRow.get(field);
      if (!Objects.equals(rowValue, columnValue)) {
        result.add(new Mismatch(pk, field, rowValue, columnValue, Mismatch.Type.FIELD_DIFF));
      }
    }
  }
}
