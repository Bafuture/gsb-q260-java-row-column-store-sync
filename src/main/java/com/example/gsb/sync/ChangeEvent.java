package com.example.gsb.sync;

import java.io.Serializable;
import java.util.Map;

/**
 * 行存投递到同步队列的一条变更事件。
 *
 * @param seq 全局单调递增序列号，用于幂等与判定窗口
 * @param pk 主键
 * @param op 变更类型
 * @param fields UPSERT 时为变更后的整行镜像；DELETE 时为空
 * @param createdAtMillis 事件产生时间（用于延迟统计）
 */
public record ChangeEvent(long seq, long pk, OperationType op, Map<String, Object> fields,
                          long createdAtMillis) implements Serializable {
  public ChangeEvent {
    fields = fields == null ? Map.of() : Map.copyOf(fields);
  }
}
