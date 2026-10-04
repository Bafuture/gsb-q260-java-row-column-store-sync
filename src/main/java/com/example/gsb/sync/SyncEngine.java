package com.example.gsb.sync;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 异步同步引擎：单线程消费同步队列，把变更应用到列存。
 *
 * <p>顺序：WAL 已包含事件 → 列存幂等应用并持久化批次 → 推进 ack。
 * 单线程按 seq 顺序应用，避免乱序覆盖；崩溃重放同样按 seq 升序。
 */
public final class SyncEngine {

    private final SyncQueue queue;
    private final ColumnStore columnStore;
    private final SyncMetrics metrics;
    private final Thread worker;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile boolean paused;

    public SyncEngine(SyncQueue queue, ColumnStore columnStore, SyncMetrics metrics) {
        this.queue = queue;
        this.columnStore = columnStore;
        this.metrics = metrics;
        this.worker = new Thread(this::run, "sync-engine");
        this.worker.setDaemon(true);
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            worker.start();
        }
    }

    public void pause() {
        paused = true;
    }

    public void resume() {
        paused = false;
    }

    public boolean isPaused() {
        return paused;
    }

    private void run() {
        while (running.get()) {
            try {
                if (paused) {
                    Thread.sleep(20);
                    continue;
                }
                ChangeEvent event = queue.poll(50);
                if (event == null) {
                    continue;
                }
                long begin = System.currentTimeMillis();
                boolean applied = columnStore.apply(event);
                if (applied) {
                    metrics.recordApplied(Math.max(0, begin - event.timestampMillis));
                } else {
                    metrics.recordDuplicate();
                }
                queue.ack(event.seq);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                // 单条应用失败不杀死消费线程；事件已在 WAL，重启后仍可重放
                if (!running.get()) {
                    return;
                }
            }
        }
    }

    /** 停止消费线程；不会主动追平未消费事件（用于模拟/真实崩溃后的关闭）。 */
    public void stop() {
        running.set(false);
        worker.interrupt();
        try {
            worker.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
