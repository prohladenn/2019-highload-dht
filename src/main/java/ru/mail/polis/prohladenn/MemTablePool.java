package ru.mail.polis.prohladenn;

import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemTablePool implements Table, Closeable {
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final long maxHeap;
    private final NavigableMap<Integer, Table> tableForFlush;
    private volatile MemTable current;
    private final BlockingQueue<TableToFlush> flushQueue;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final AtomicBoolean compacting = new AtomicBoolean(false);
    private final AtomicInteger fileIndex;

    MemTablePool(final long maxHeap, @NotNull final AtomicInteger fileIndex) {
        this.maxHeap = maxHeap;
        this.current = new MemTable();
        this.tableForFlush = new ConcurrentSkipListMap<>();
        this.flushQueue = new ArrayBlockingQueue<>(2);
        this.fileIndex = fileIndex;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        lock.readLock().lock();
        final List<Iterator<Cell>> iteratorList;
        try {
            iteratorList = Utils.getListIterators(tableForFlush, current, from);
        } finally {
            lock.readLock().unlock();
        }
        return Utils.getActualRowIterator(iteratorList);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key,
                       @NotNull final ByteBuffer value,
                       @NotNull final AtomicInteger fileIndex) {
        if (stop.get()) {
            throw new IllegalStateException("Already stopped");
        }
        current.upsert(key, value, fileIndex);
        enqueueFlush(fileIndex);

    }

    @Override
    public void remove(@NotNull final ByteBuffer key,
                       @NotNull final AtomicInteger fileIndex) {
        if (stop.get()) {
            throw new IllegalStateException("Already stopped");
        }
        current.remove(key, fileIndex);
        enqueueFlush(fileIndex);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sizeInBytes() {
        lock.readLock().lock();
        try {
            long sizeInBytes = 0;
            sizeInBytes += current.sizeInBytes();
            for (final Map.Entry<Integer, Table> entry : tableForFlush.entrySet()) {
                sizeInBytes += entry.getValue().sizeInBytes();
            }
            return sizeInBytes;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        if (!stop.compareAndSet(false, true)) {
            return;
        }
        lock.writeLock().lock();
        TableToFlush table;
        try {
            table = new TableToFlush(current, fileIndex.get(), true);
        } finally {
            lock.writeLock().unlock();
        }
        try {
            flushQueue.put(table);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    void compact() {
        if (!compacting.compareAndSet(false, true)) {
            return;
        }
        lock.writeLock().lock();
        TableToFlush table;
        try {
            table = new TableToFlush(current, fileIndex.getAndAdd(1), true, true);
            tableForFlush.put(table.getFileIndex(), table.getTable());
            current = new MemTable();
        } finally {
            lock.writeLock().unlock();
        }
        try {
            flushQueue.put(table);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void enqueueFlush(@NotNull final AtomicInteger fileIndex) {
        if (current.sizeInBytes() >= maxHeap) {
            TableToFlush table = null;
            int index;
            lock.writeLock().lock();
            try {
                if (current.sizeInBytes() >= maxHeap) {
                    index = fileIndex.getAndAdd(1);
                    table = new TableToFlush(current, index);
                    tableForFlush.put(index, current);
                    current = new MemTable();
                }
            } finally {
                lock.writeLock().unlock();
            }
            if (table != null) {
                try {
                    flushQueue.put(table);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    TableToFlush takeToFlush() throws InterruptedException {
        return flushQueue.take();
    }

    void flushed(final int generation) {
        lock.writeLock().lock();
        try {
            tableForFlush.remove(generation);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
