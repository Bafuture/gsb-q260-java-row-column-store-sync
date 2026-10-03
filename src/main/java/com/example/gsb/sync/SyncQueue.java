package com.example.gsb.sync;

/**
 * 行存 → 列存的变更投递队列。
 *
 * <p>语义要求：
 * <ul>
 *   <li>{@link #append(ChangeEvent)} 必须持久化后才返回（崩溃不丢）；</li>
 *   <li>单消费者按 seq 顺序 {@link #poll()}，处理成功后 {@link #ack(long)}；</li>
 *   <li>ack 之前崩溃，重启后同一条事件会被重新投递（at-least-once），
 *       由列存依据 seq 做幂等，实现“补齐且不重复应用”。</li>
 * </ul>
 */
public interface SyncQueue {

  void append(ChangeEvent event);

  /** 取出下一条未消费事件，没有则返回 null。 */
  ChangeEvent poll();

  /** 标记 seq（含）之前的事件已成功应用到列存。 */
  void ack(long seq);

  /** 当前未 ack 的事件数。 */
  int pendingCount();

  /** 最近一次已持久化 ack 的 seq，重启后从此水位恢复。 */
  long ackedSeq();

  void close();
}
