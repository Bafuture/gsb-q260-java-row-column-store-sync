package com.example.gsb.sync;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 同步队列：WAL（追加写日志）+ 已消费位点（ack）。
 *
 * <p>每条变更先同步落盘 WAL 再对消费者可见；消费者应用到列存并持久化列存批次后
 * 才推进 ack。崩溃时：
 * <ul>
 *   <li>WAL 尾部写了一半的残记录在加载时按长度截断丢弃；</li>
 *   <li>seq &gt; ack 的事件会被重放补齐；</li>
 *   <li>应用本身按 seq 幂等，所以"应用成功、ack 未及落盘"的事件重放也不会重复生效。</li>
 * </ul>
 */
public final class SyncQueue {

    private final Path walFile;
    private final Path ackFile;
    private final Object writeLock = new Object();
    private final LinkedBlockingQueue<ChangeEvent> pending = new LinkedBlockingQueue<>();

    private volatile long ackSeq;
    private volatile long maxSeq;

    public SyncQueue(Path dataDir) {
        Path dir = dataDir.resolve("queue");
        this.walFile = dir.resolve("wal.log");
        this.ackFile = dir.resolve("sync.ack");
        try {
            Files.createDirectories(dir);
            load();
        } catch (IOException e) {
            throw new IllegalStateException("init sync queue failed: " + dir, e);
        }
    }

    private void load() throws IOException {
        List<ChangeEvent> all = new ArrayList<>();
        if (Files.exists(walFile)) {
            try (DataInputStream in = new DataInputStream(
                    new BufferedInputStream(Files.newInputStream(walFile)))) {
                while (true) {
                    ChangeEvent event;
                    try {
                        int len = in.readInt();
                        byte[] payload = in.readNBytes(len);
                        if (payload.length != len) {
                            break; // 崩溃导致的截断尾部，丢弃
                        }
                        event = ChangeEvent.fromBytes(payload);
                    } catch (EOFException e) {
                        break;
                    }
                    all.add(event);
                }
            }
        }
        this.ackSeq = readAck();
        this.maxSeq = all.isEmpty() ? 0L : all.get(all.size() - 1).seq;
        for (ChangeEvent e : all) {
            if (e.seq > ackSeq) {
                pending.add(e);
            }
        }
    }

    private long readAck() {
        if (!Files.exists(ackFile)) {
            return 0L;
        }
        try {
            List<String> lines = Files.readAllLines(ackFile);
            return lines.isEmpty() ? 0L : Long.parseLong(lines.get(0).trim());
        } catch (IOException | NumberFormatException e) {
            return 0L;
        }
    }

    /** 返回 WAL 中的全部事件（用于崩溃后重建行存内存状态），按 seq 排序。 */
    public List<ChangeEvent> replayAll() throws IOException {
        List<ChangeEvent> all = new ArrayList<>();
        if (!Files.exists(walFile)) {
            return all;
        }
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(walFile)))) {
            while (true) {
                int len;
                try {
                    len = in.readInt();
                } catch (EOFException e) {
                    break;
                }
                byte[] payload = in.readNBytes(len);
                if (payload.length != len) {
                    break;
                }
                all.add(ChangeEvent.fromBytes(payload));
            }
        }
        all.sort((a, b) -> Long.compare(a.seq, b.seq));
        return all;
    }

    /** 追加一条事件：同步刷盘 WAL，随后对消费者可见。 */
    public ChangeEvent append(ChangeEvent event) {
        byte[] payload = event.toBytes();
        synchronized (writeLock) {
            try (OutputStream out = Files.newOutputStream(walFile,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                out.write(intBytes(payload.length));
                out.write(payload);
                out.flush();
            } catch (IOException e) {
                throw new IllegalStateException("append wal failed", e);
            }
            if (event.seq > maxSeq) {
                maxSeq = event.seq;
            }
        }
        pending.offer(event);
        return event;
    }

    private static byte[] intBytes(int v) {
        return new byte[] {
                (byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v
        };
    }

    public ChangeEvent poll(long timeoutMillis) throws InterruptedException {
        return pending.poll(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    /** 持久化消费位点（临时文件 + 原子 rename，避免崩溃写出半截 ack）。 */
    public void ack(long seq) {
        try {
            Path tmp = ackFile.resolveSibling("sync.ack.tmp");
            Files.writeString(tmp, Long.toString(seq));
            Files.move(tmp, ackFile, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
            this.ackSeq = seq;
        } catch (IOException e) {
            throw new IllegalStateException("persist ack failed", e);
        }
    }

    public long ackSeq() {
        return ackSeq;
    }

    public long maxSeq() {
        return maxSeq;
    }

    public int pendingCount() {
        return pending.size();
    }
}
