package com.example.gsb.sync;

/**
 * 异步同步消费者：轮询队列 → 应用列存 → ack。
 *
 * <p>ack 在应用成功之后执行，因此崩溃会导致事件重投（at-least-once），
 * 列存按 seq 幂等跳过，统计为 {@code duplicateSkipped}。
 */
public class SyncWorker implements AutoCloseable {

  private final SyncQueue queue;
  private final ColumnStore columnStore;
  private final SyncStats stats;
  private final long pollIntervalMillis;
  private final Object lock = new Object();
  private volatile boolean running;
  private Thread thread;

  public SyncWorker(SyncQueue queue, ColumnStore columnStore, SyncStats stats,
                    long pollIntervalMillis) {
    this.queue = queue;
    this.columnStore = columnStore;
    this.stats = stats;
    this.pollIntervalMillis = pollIntervalMillis;
  }

  public void start() {
    running = true;
    thread = new Thread(this::loop, "sync-worker");
    thread.setDaemon(true);
    thread.start();
  }

  private void loop() {
    while (running) {
      if (!processOne()) {
        try {
          Thread.sleep(pollIntervalMillis);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  private boolean processOne() {
    synchronized (lock) {
      ChangeEvent event = queue.poll();
      if (event == null) {
        return false;
      }
      boolean applied = columnStore.apply(event);
      if (applied) {
        stats.recordSync(System.currentTimeMillis() - event.createdAtMillis());
      } else {
        stats.recordDuplicateSkipped();
      }
      queue.ack(event.seq());
      return true;
    }
  }

  /** 同步阻塞地把当前积压事件全部消费完，返回处理条数。 */
  public int drain() {
    int processed = 0;
    while (processOne()) {
      processed++;
    }
    return processed;
  }

  /** 等待异步消费追平（队列空且无在途处理）。 */
  public boolean awaitIdle(long timeoutMillis) {
    long deadline = System.currentTimeMillis() + timeoutMillis;
    while (System.currentTimeMillis() <= deadline) {
      if (queue.pendingCount() == 0) {
        synchronized (lock) {
          if (queue.pendingCount() == 0) {
            return true;
          }
        }
      }
      try {
        Thread.sleep(2);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return false;
  }

  @Override
  public void close() {
    running = false;
    if (thread != null) {
      thread.interrupt();
      try {
        thread.join(2000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
