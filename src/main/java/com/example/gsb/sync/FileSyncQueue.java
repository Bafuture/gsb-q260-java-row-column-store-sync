package com.example.gsb.sync;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * 基于本地文件的持久化队列：长度前缀追加日志 + ack 水位文件。
 *
 * <p>每次 append 都 flush 到磁盘；每次 ack 都覆盖写 {@code ack.txt}。
 * 崩溃恢复时重放 events.log，只把 seq &gt; ackedSeq 的事件放回待消费队列。
 * 尾部若存在崩溃时写了一半的残缺记录（EOF），直接忽略。
 */
public final class FileSyncQueue implements SyncQueue {

  private static final String LOG_FILE = "events.log";
  private static final String ACK_FILE = "ack.txt";

  private final Path logPath;
  private final Path ackPath;
  private final DataOutputStream out;
  private final Deque<ChangeEvent> pending = new ArrayDeque<>();
  private long ackedSeq;

  public FileSyncQueue(Path directory) throws IOException {
    Files.createDirectories(directory);
    this.logPath = directory.resolve(LOG_FILE);
    this.ackPath = directory.resolve(ACK_FILE);
    this.ackedSeq = readAck();
    replayLog();
    this.out = new DataOutputStream(new BufferedOutputStream(
        Files.newOutputStream(logPath,
            StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)));
  }

  private long readAck() throws IOException {
    if (Files.notExists(ackPath) || Files.size(ackPath) == 0) {
      return 0L;
    }
    String text = Files.readString(ackPath).trim();
    return text.isEmpty() ? 0L : Long.parseLong(text);
  }

  private void replayLog() throws IOException {
    if (Files.notExists(logPath)) {
      return;
    }
    long validLength = 0L;
    boolean truncated = false;
    try (RandomAccessFile raf = new RandomAccessFile(logPath.toFile(), "rw")) {
      while (true) {
        long recordStart = raf.getFilePointer();
        try {
          int length = raf.readInt();
          byte[] body = new byte[length];
          raf.readFully(body);
          ChangeEvent event = deserialize(body);
          if (event.seq() > ackedSeq) {
            pending.add(event);
          }
          validLength = raf.getFilePointer();
        } catch (EOFException eof) {
          if (raf.getFilePointer() > recordStart || raf.getFilePointer() != validLength) {
            truncated = true;
          }
          break;
        } catch (IOException | RuntimeException corrupted) {
          truncated = true;
          break;
        }
      }
      if (raf.length() > validLength) {
        truncated = true;
      }
      if (truncated) {
        raf.setLength(validLength);
      }
    }
  }

  @Override
  public synchronized void append(ChangeEvent event) {
    try {
      byte[] body = serialize(event);
      out.writeInt(body.length);
      out.write(body);
      out.flush();
    } catch (IOException e) {
      throw new IllegalStateException("同步事件持久化失败: " + event, e);
    }
    pending.add(event);
  }

  @Override
  public synchronized ChangeEvent poll() {
    return pending.poll();
  }

  @Override
  public synchronized void ack(long seq) {
    if (seq <= ackedSeq) {
      return;
    }
    ackedSeq = seq;
    try {
      Path tmp = ackPath.resolveSibling(ACK_FILE + ".tmp");
      Files.writeString(tmp, Long.toString(ackedSeq));
      Files.move(tmp, ackPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      throw new IllegalStateException("ack 水位持久化失败", e);
    }
  }

  @Override
  public synchronized int pendingCount() {
    return pending.size();
  }

  @Override
  public synchronized long ackedSeq() {
    return ackedSeq;
  }

  @Override
  public synchronized void close() {
    try {
      out.close();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static byte[] serialize(ChangeEvent event) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(event);
    }
    return bos.toByteArray();
  }

  private static ChangeEvent deserialize(byte[] body) throws IOException {
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(body))) {
      return (ChangeEvent) ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException("无法反序列化同步事件", e);
    }
  }
}
