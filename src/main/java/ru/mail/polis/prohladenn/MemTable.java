package ru.mail.polis.prohladenn;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MemTable implements Table {
    private final SortedMap<ByteBuffer, Cell> memTable = new ConcurrentSkipListMap<>();
    private final AtomicLong currentHeap = new AtomicLong(0);

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return memTable.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value,
                       @NotNull final AtomicInteger fileIndex) {
        final Cell previousCell = memTable.put(key, Cell.of(fileIndex.get(), key, value, ThreadSafeDAO.ALIVE));
        if (previousCell == null) {
            currentHeap.addAndGet(Integer.BYTES
                    + (long) (key.remaining() + ThreadSafeDAO.LINK_SIZE
                    + Integer.BYTES * ThreadSafeDAO.NUMBER_FIELDS)
                    + (long) (value.remaining() + ThreadSafeDAO.LINK_SIZE
                    + Integer.BYTES * ThreadSafeDAO.NUMBER_FIELDS)
                    + Integer.BYTES);
        } else {
            currentHeap.addAndGet(value.remaining() - previousCell.getValue().remaining());
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key,
                       @NotNull final AtomicInteger fileIndex) {
        final Cell removedCell = memTable.put(key, Cell.of(fileIndex.get(), key, ThreadSafeDAO.TOMBSTONE, ThreadSafeDAO.DEAD));
        if (removedCell == null) {
            currentHeap.addAndGet(Integer.BYTES
                    + (long) (key.remaining() + ThreadSafeDAO.LINK_SIZE
                    + Integer.BYTES * ThreadSafeDAO.NUMBER_FIELDS)
                    + (long) (ThreadSafeDAO.LINK_SIZE
                    + Integer.BYTES * ThreadSafeDAO.NUMBER_FIELDS)
                    + Integer.BYTES);
        } else if (!removedCell.isDead()) {
            currentHeap.addAndGet(-removedCell.getValue().remaining());
        }
    }

    @Override
    public void clear() {
        memTable.clear();
        currentHeap.set(0);
    }

    @Override
    public long sizeInBytes() {
        return currentHeap.get();
    }
}
