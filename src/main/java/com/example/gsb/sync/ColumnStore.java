package com.example.gsb.sync;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 列式存储：按主键范围切分成多个批次（batch）。
 *
 * <ul>
 *   <li>每条变更按 seq 幂等应用（seq 不新于批次内记录即跳过，即"不重复应用"）；</li>
 *   <li>每次应用写穿持久化批次快照（tmp + 原子 rename），且先于同步位点 ack；</li>
 *   <li>批次支持单独重建：先在旁构建新快照，重建期间查询走旧批次并可见
 *       {@link BatchState#REBUILDING} 状态，最后在批次锁内合并新/旧版本后原子切换；</li>
 *   <li>删除以墓碑 Entry（fields == null）留在批次内，保证重建并发期不会"复活"。</li>
 * </ul>
 */
public final class ColumnStore {

    public enum BatchState {
        ACTIVE,
        REBUILDING
    }

    /** 批次内一条记录；fields 为 null 表示删除墓碑。 */
    public record Entry(Map<String, String> fields, long lastSeq) {
        boolean deleted() {
            return fields == null;
        }
    }

    public static final class Batch {
        private final long batchId;
        private volatile Map<Long, Entry> data;
        private volatile BatchState state = BatchState.ACTIVE;
        private final Object lock = new Object();

        Batch(long batchId, Map<Long, Entry> data) {
            this.batchId = batchId;
            this.data = data;
        }

        public long batchId() {
            return batchId;
        }

        public BatchState state() {
            return state;
        }

        public Optional<Entry> get(long key) {
            return Optional.ofNullable(data.get(key));
        }
    }

    private final long batchSize;
    private final Path batchDir;
    private final ConcurrentHashMap<Long, Batch> batches = new ConcurrentHashMap<>();

    public ColumnStore(Path dataDir, long batchSize) {
        this.batchSize = batchSize;
        this.batchDir = dataDir.resolve("column");
        try {
            Files.createDirectories(batchDir);
            load();
        } catch (IOException e) {
            throw new IllegalStateException("init column store failed", e);
        }
    }

    public long batchIdOf(long key) {
        return Math.floorDiv(key, batchSize);
    }

    private Batch batch(long key) {
        return batches.computeIfAbsent(batchIdOf(key),
                id -> new Batch(id, Collections.emptyMap()));
    }

    private Batch batchById(long batchId) {
        return batches.computeIfAbsent(batchId,
                id -> new Batch(id, Collections.emptyMap()));
    }

    /**
     * 应用一条变更，返回 true 表示真正生效，false 表示因 seq 过旧被幂等去重。
     */
    public boolean apply(ChangeEvent event) {
        Batch b = batch(event.key);
        synchronized (b.lock) {
            Entry current = b.data.get(event.key);
            if (current != null && current.lastSeq >= event.seq) {
                return false;
            }
            Map<Long, Entry> next = new HashMap<>(b.data);
            Map<String, String> fields = event.type == ChangeEvent.Type.DELETE
                    ? null : event.fields;
            next.put(event.key, new Entry(fields, event.seq));
            b.data = Collections.unmodifiableMap(next);
            persist(b);
            return true;
        }
    }

    /**
     * 只重建指定批次。重建期间批次标记为 REBUILDING，查询仍返回旧批次数据。
     *
     * @param rebuildingHook 快照已构建、尚未切换前回调（测试用，可为 null），
     *                       可在此阻塞以模拟长时间重建
     */
    public void rebuildBatch(long batchId, RowStore rowStore, Runnable rebuildingHook) {
        Batch b = batchById(batchId);
        synchronized (b.lock) {
            if (b.state == BatchState.REBUILDING) {
                throw new IllegalStateException("batch " + batchId + " already rebuilding");
            }
            b.state = BatchState.REBUILDING;
        }

        long from = batchId * batchSize;
        long to = from + batchSize - 1;
        Map<Long, Entry> snapshot = new HashMap<>();
        for (RowStore.RowRecord r : rowStore.rowsInRange(from, to)) {
            snapshot.put(r.key(), new Entry(r.fields(), r.lastSeq()));
        }

        if (rebuildingHook != null) {
            rebuildingHook.run();
        }

        synchronized (b.lock) {
            // 并发期间变更可能已应用到旧批次；按 seq 合并更新版本（含删除墓碑）
            for (Map.Entry<Long, Entry> e : b.data.entrySet()) {
                Entry fromSnapshot = snapshot.get(e.getKey());
                if (fromSnapshot == null || e.getValue().lastSeq() > fromSnapshot.lastSeq()) {
                    snapshot.put(e.getKey(), e.getValue());
                }
            }
            b.data = Collections.unmodifiableMap(new TreeMap<>(snapshot));
            persist(b);
            b.state = BatchState.ACTIVE;
            b.lock.notifyAll();
        }
    }

    public BatchState stateOf(long key) {
        return batch(key).state();
    }

    public BatchState stateOfBatch(long batchId) {
        return batchById(batchId).state();
    }

    /** 查询单条记录；返回结果带批次状态，重建期间可返回旧批次数据或重建中标记。 */
    public QueryResult query(long key) {
        Batch b = batch(key);
        Entry e = b.data.get(key);
        if (e == null || e.deleted()) {
            return new QueryResult(b.state, Optional.empty());
        }
        return new QueryResult(b.state, Optional.of(e.fields()));
    }

    public record QueryResult(BatchState batchState, Optional<Map<String, String>> row) {
    }

    /** 范围内部主键（活记录），供一致性检查使用。 */
    Map<Long, Entry> liveEntriesInRange(long fromKeyInclusive, long toKeyInclusive) {
        Map<Long, Entry> result = new TreeMap<>();
        for (Long key : keysToConsider(fromKeyInclusive, toKeyInclusive)) {
            Entry e = batch(key).data.get(key);
            if (e != null && !e.deleted()) {
                result.put(key, e);
            }
        }
        return result;
    }

    private java.util.List<Long> keysToConsider(long from, long to) {
        java.util.List<Long> keys = new java.util.ArrayList<>();
        long firstBatch = batchIdOf(from);
        long lastBatch = batchIdOf(to);
        for (long id = firstBatch; id <= lastBatch; id++) {
            Batch b = batches.get(id);
            if (b == null) {
                continue;
            }
            for (long k : b.data.keySet()) {
                if (k >= from && k <= to) {
                    keys.add(k);
                }
            }
        }
        return keys;
    }

    /** 测试钩子：直接篡改批次内字段值，模拟磁盘/内存损坏（不改 seq）。 */
    void corruptForTest(long key, String field, String badValue) {
        Batch b = batch(key);
        synchronized (b.lock) {
            Entry current = b.data.get(key);
            if (current == null || current.deleted()) {
                throw new IllegalStateException("cannot corrupt absent key " + key);
            }
            Map<String, String> corrupted = new LinkedHashMap<>(current.fields());
            corrupted.put(field, badValue);
            Map<Long, Entry> next = new HashMap<>(b.data);
            next.put(key, new Entry(Collections.unmodifiableMap(corrupted), current.lastSeq()));
            b.data = Collections.unmodifiableMap(next);
            persist(b);
        }
    }

    /** 测试钩子：从批次中抹掉一条记录，模拟列存数据丢失。 */
    void removeForTest(long key) {
        Batch b = batch(key);
        synchronized (b.lock) {
            Map<Long, Entry> next = new HashMap<>(b.data);
            next.remove(key);
            b.data = Collections.unmodifiableMap(next);
            persist(b);
        }
    }

    /** 测试钩子：向批次塞入一条行存没有的记录，模拟列存多出的脏数据。 */
    void insertForTest(long key, Map<String, String> fields, long seq) {
        Batch b = batch(key);
        synchronized (b.lock) {
            Map<Long, Entry> next = new HashMap<>(b.data);
            next.put(key, new Entry(Collections.unmodifiableMap(new LinkedHashMap<>(fields)), seq));
            b.data = Collections.unmodifiableMap(next);
            persist(b);
        }
    }

    // ---- 持久化 ----

    private void load() throws IOException {
        try (var files = Files.list(batchDir)) {
            files.filter(f -> f.getFileName().toString().startsWith("batch-")
                            && f.getFileName().toString().endsWith(".dat"))
                 .forEach(this::loadFile);
        }
    }

    private void loadFile(Path file) {
        String name = file.getFileName().toString();
        long batchId = Long.parseLong(name.substring("batch-".length(),
                name.length() - ".dat".length()));
        Map<Long, Entry> data = new HashMap<>();
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(file)))) {
            int magic = in.readInt();
            if (magic != MAGIC) {
                throw new IOException("bad magic in " + file);
            }
            long fileBatchId = in.readLong();
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                long key = in.readLong();
                long seq = in.readLong();
                byte deleted = in.readByte();
                Map<String, String> fields = null;
                if (deleted == 0) {
                    int n = in.readInt();
                    fields = new LinkedHashMap<>();
                    for (int j = 0; j < n; j++) {
                        fields.put(in.readUTF(), in.readUTF());
                    }
                }
                data.put(key, new Entry(fields == null ? null
                        : Collections.unmodifiableMap(fields), seq));
            }
            if (fileBatchId != batchId) {
                throw new IOException("batch id mismatch in " + file);
            }
        } catch (EOFException ignored) {
            // 写入中途崩溃的残批次快照直接忽略，等待重建或 WAL 重放
            return;
        } catch (IOException e) {
            throw new IllegalStateException("load batch " + file + " failed", e);
        }
        batches.put(batchId, new Batch(batchId, Collections.unmodifiableMap(new TreeMap<>(data))));
    }

    private static final int MAGIC = 0xC0A1_0001;

    private void persist(Batch b) {
        Path target = batchDir.resolve("batch-" + b.batchId + ".dat");
        Path tmp = batchDir.resolve("batch-" + b.batchId + ".dat.tmp");
        Map<Long, Entry> snapshot = b.data;
        try (OutputStream os = Files.newOutputStream(tmp);
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(os))) {
            out.writeInt(MAGIC);
            out.writeLong(b.batchId);
            out.writeInt(snapshot.size());
            for (Map.Entry<Long, Entry> e : snapshot.entrySet()) {
                out.writeLong(e.getKey());
                out.writeLong(e.getValue().lastSeq());
                if (e.getValue().deleted()) {
                    out.writeByte(1);
                } else {
                    out.writeByte(0);
                    out.writeInt(e.getValue().fields().size());
                    for (Map.Entry<String, String> f : e.getValue().fields().entrySet()) {
                        out.writeUTF(f.getKey());
                        out.writeUTF(f.getValue());
                    }
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("persist batch " + b.batchId + " failed", e);
        }
        try {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new IllegalStateException("swap batch snapshot failed", e);
        }
    }
}
