package ru.mail.polis.prohladenn;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public final class MemTable implements Table {
    private final SortedMap<ByteBuffer, Value> map = new ConcurrentSkipListMap<>();
    private final AtomicLong sizeInBytes = new AtomicLong();
    private final AtomicLong generation = new AtomicLong();

    MemTable(final long generation) {
        this.generation.set(generation);
    }

    @Override
    public long sizeInBytes() {
        return sizeInBytes.get();
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return Iterators.transform(
                map.tailMap(from).entrySet().stream()
                        .filter(e -> e.getValue().getTimeStamp() <= System.nanoTime()).iterator(),
                e -> new Cell(e.getKey(), e.getValue()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        final Value previous = map.put(key, Value.of(value));
        upsert(key, value, previous);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value, @NotNull final Duration ttl) {
        final Value previous = map.put(key, Value.tombstone(ttl.toMillis()));
        upsert(key, value, previous);
    }

    private void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value, final Value previous) {
        if (previous == null) {
            sizeInBytes.addAndGet(key.remaining() + value.remaining());
        } else if (previous.isRemoved()) {
            sizeInBytes.addAndGet(value.remaining());
        } else {
            sizeInBytes.addAndGet(value.remaining() - previous.getData().remaining());
        }
    }

    @Override
    public boolean contains(@NotNull final ByteBuffer key) {
        return map.containsKey(key);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        final Value previous = map.put(key, Value.tombstone());
        if (previous == null) {
            sizeInBytes.addAndGet(key.remaining());
        } else if (!previous.isRemoved()) {
            sizeInBytes.addAndGet(-previous.getData().remaining());
        }
    }
}
