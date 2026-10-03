package com.example.gsb.sync;

/** 一次一致性检查发现的不一致项。 */
public record Mismatch(long pk, String field, Object rowValue, Object columnValue, Type type) {

  public enum Type {
    /** 行存有、列存无。 */
    ONLY_IN_ROW,
    /** 列存有、行存无（或已删除未同步）。 */
    ONLY_IN_COLUMN,
    /** 两边都有但字段值不同。 */
    FIELD_DIFF
  }

  /** 整行级不一致时 field 用 "*"。 */
  public static final String WHOLE_ROW = "*";
}
