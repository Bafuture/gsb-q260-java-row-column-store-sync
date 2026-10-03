package com.example.gsb.sync;

/** 行存变更类型。 */
public enum OperationType {
  /** 插入或更新（携带变更后整行镜像，天然幂等）。 */
  UPSERT,
  /** 按主键删除。 */
  DELETE
}
