package ru.mail.polis.prohladenn;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.Iters;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemTablePool implements Table, Closeable {

    private volatile MemTable currentMemTable;
    private final NavigableMap<Long, Table> pendingToFlushTables;
    private long generation;
    private final long memFlushThreshold;
    private final BlockingQueue<TableToFlush> flushingQueue;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final AtomicBoolean stop = new AtomicBoolean();

    /**
     * Combined memTables.
     *
     * @param startGeneration   generation
     * @param memFlushThreshold threshold when tables need to be flushed
     */
    public MemTablePool(final long startGeneration,
                        final long memFlushThreshold) {
        this.generation = startGeneration;
        this.memFlushThreshold = memFlushThreshold;
        this.currentMemTable = new MemTable(generation);
        this.pendingToFlushTables = new TreeMap<>();
        this.flushingQueue = new ArrayBlockingQueue<>(2);
    }

    @Override
    public long sizeInBytes() {
        lock.readLock().lock();
        try {
            return currentMemTable.sizeInBytes();
        } finally {
            lock.readLock().unlock();
        }
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(final @NotNull ByteBuffer from) {
        final Collection<Iterator<Cell>> iterators;
        lock.readLock().lock();
        try {

            iterators = new ArrayList<>(pendingToFlushTables.size() + 1);
            for (final Table table : pendingToFlushTables.descendingMap().values()) {
                iterators.add(table.iterator(from));
            }
            iterators.add(currentMemTable.iterator(from));
        } finally {
            lock.readLock().unlock();
        }
        final Iterator<Cell> mergeIterator = Iterators.mergeSorted(iterators, Cell.COMPARATOR);
        return Iters.collapseEquals(mergeIterator, Cell::getKey);
    }

    private void enqueueFlush() {
        TableToFlush tableToFlush = null;
        lock.writeLock().lock();
        try {

            if (currentMemTable.sizeInBytes() > memFlushThreshold) {
                tableToFlush = new TableToFlush(generation,
                        currentMemTable.iterator(LSMDao.EMPTY),
                        false);
                pendingToFlushTables.put(generation, currentMemTable);
                generation = generation + 1;
                currentMemTable = new MemTable(generation);
            }
        } finally {
            lock.writeLock().unlock();
        }
        if (tableToFlush != null) {
            try {
                flushingQueue.put(tableToFlush);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void upsert(final @NotNull ByteBuffer key, final @NotNull ByteBuffer value) {
        if (stop.get()) {
            throw new IllegalStateException("Already stopped!");
        }
        currentMemTable.upsert(key, value);
        enqueueFlush();
    }

    @Override
    public void remove(final @NotNull ByteBuffer key) {
        if (stop.get()) {
            throw new IllegalStateException("Already stopped!");
        }
        currentMemTable.remove(key);
        enqueueFlush();
    }

    public TableToFlush takeToFlush() throws InterruptedException {
        return flushingQueue.take();
    }

    /**
     * Removes flushed tables from pendingToFlushTables.
     *
     * @param generation generation of tables
     */
    public void flushed(final long generation) {
        lock.writeLock().lock();
        try {
            pendingToFlushTables.remove(generation);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        if (!stop.compareAndSet(false, true)) {
            return;
        }
        TableToFlush tableToFlush;
        lock.writeLock().lock();
        try {
            tableToFlush = new TableToFlush(generation, currentMemTable.iterator(LSMDao.EMPTY), true, false);
        } finally {
            lock.writeLock().unlock();
        }

        try {
            flushingQueue.put(tableToFlush);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * MemTables compaction.
     *
     * @param fileTables collection of fileTables
     * @param generation generation of fileTables
     * @param base       directory
     * @throws IOException if an I/O error occurred
     */
    public void compact(@NotNull final Collection<FileTable> fileTables,
                        final long generation,
                        final File base) throws IOException {
        final Iterator<Cell> alive;
        lock.readLock().lock();
        try {
            alive = IterUtils.collapse(currentMemTable, fileTables, LSMDao.EMPTY);
        } finally {
            lock.readLock().unlock();
        }
        final File tmp = new File(base, generation + LSMDao.TABLE + LSMDao.TEMP);
        FileTable.write(alive, tmp);
        lock.readLock().lock();
        try {
            for (final FileTable fileTable : fileTables) {
                Files.delete(fileTable.getPath());
            }
            fileTables.clear();
            final File file = new File(base, generation + LSMDao.TABLE + LSMDao.SUFFIX);
            Files.move(tmp.toPath(), file.toPath(), StandardCopyOption.ATOMIC_MOVE);
            fileTables.add(new FileTable(file));
        } finally {
            lock.readLock().unlock();
        }
    }
}
