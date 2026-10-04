package com.example.gsb.sync;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 行列双存同步组件门面。
 *
 * <p>写入路径：行存点写（内存）→ 变更事件同步落盘 WAL → 后台引擎异步应用到列存。
 * 因此行存立即可读，列存最终一致；一致性检查通过判定窗口过滤正常同步延迟。
 *
 * <p>崩溃恢复：重启时用 WAL 重建行存内存状态，列存批次从磁盘快照加载，
 * 再重放 ack 之后的事件补齐；应用按 seq 幂等，保证"补齐且不重复应用"。
 */
public final class DualStore implements Closeable {

    public record Config(long batchSize, long lagToleranceMillis,
                         long tombstoneRetentionMillis) {
        public static Config defaults() {
            return new Config(1000, 1000, 60_000);
        }
    }

    private final RowStore rowStore = new RowStore();
    private final SyncQueue queue;
    private final ColumnStore columnStore;
    private final SyncMetrics metrics = new SyncMetrics();
    private final SyncEngine engine;
    private final ConsistencyChecker checker;
    private final AtomicLong seq = new AtomicLong();
    private volatile Runnable rebuildHook;

    public DualStore(Path dataDir) {
        this(dataDir, Config.defaults());
    }

    public DualStore(Path dataDir, Config config) {
        this.queue = new SyncQueue(dataDir);
        this.seq.set(queue.maxSeq());
        try {
            for (ChangeEvent e : queue.replayAll()) {
                rowStore.apply(e);
            }
        } catch (IOException e) {
            throw new IllegalStateException("replay wal into row store failed", e);
        }
        this.columnStore = new ColumnStore(dataDir, config.batchSize());
        this.engine = new SyncEngine(queue, columnStore, metrics);
        this.checker = new ConsistencyChecker(rowStore, columnStore, queue, metrics,
                config.lagToleranceMillis(), config.tombstoneRetentionMillis());
        this.engine.start();
    }

    // ---- 行存写入（同步返回，列存异步追平） ----

    public void upsert(long key, Map<String, String> fields) {
        ChangeEvent event = ChangeEvent.upsert(seq.incrementAndGet(), key, fields);
        rowStore.apply(event);
        queue.append(event);
    }

    public void delete(long key) {
        ChangeEvent event = ChangeEvent.delete(seq.incrementAndGet(), key);
        rowStore.apply(event);
        queue.append(event);
    }

    // ---- 查询 ----

    public Optional<Map<String, String>> rowGet(long key) {
        return rowStore.get(key).map(RowStore.RowRecord::fields);
    }

    /** 列存查询；重建期间返回旧批次数据，状态为 REBUILDING。 */
    public ColumnStore.QueryResult columnGet(long key) {
        return columnStore.query(key);
    }

    public ColumnStore.BatchState batchStateOf(long key) {
        return columnStore.stateOf(key);
    }

    // ---- 运维操作 ----

    /** 只重建指定批次（如该批次数据损坏），重建期间查询仍可用。 */
    public void rebuildBatch(long batchId) {
        metrics.recordRebuild();
        columnStore.rebuildBatch(batchId, rowStore, rebuildHook);
    }

    public long batchIdOf(long key) {
        return columnStore.batchIdOf(key);
    }

    public ConsistencyChecker.ConsistencyReport checkConsistency(long fromKey, long toKey) {
        return checker.check(fromKey, toKey);
    }

    public SyncMetrics metrics() {
        return metrics;
    }

    // ---- 测试/调试用钩子 ----

    /** 暂停/恢复同步引擎（暂停期间事件积压在 WAL，用于模拟延迟与崩溃）。 */
    public void pauseSync() {
        engine.pause();
    }

    public void resumeSync() {
        engine.resume();
    }

    /** 等待同步引擎追平当前已写入的全部事件。 */
    public boolean awaitSynced(long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (queue.ackSeq() >= queue.maxSeq()) {
                return true;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return queue.ackSeq() >= queue.maxSeq();
    }

    /** 测试钩子：篡改列存某主键的字段值，模拟批次数据损坏。 */
    public void corruptColumnForTest(long key, String field, String badValue) {
        columnStore.corruptForTest(key, field, badValue);
    }

    /** 测试钩子：抹掉列存某主键，模拟列存数据丢失。 */
    public void removeColumnEntryForTest(long key) {
        columnStore.removeForTest(key);
    }

    /** 测试钩子：向列存塞入行存没有的数据，模拟列存脏数据。 */
    public void insertColumnEntryForTest(long key, Map<String, String> fields, long seq) {
        columnStore.insertForTest(key, fields, seq);
    }

    /** 测试钩子：重建批次时在新快照切换前回调（可阻塞以模拟长时间重建）。 */
    public void setRebuildHook(Runnable hook) {
        this.rebuildHook = hook;
    }

    @Override
    public void close() {
        engine.stop();
    }
}
