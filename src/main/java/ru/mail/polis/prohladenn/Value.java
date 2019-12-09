package ru.mail.polis.prohladenn;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Value implements Comparable<Value> {
    private static final long MILLI_TO_NANO = 1000000;

    private final long ts;
    private final ByteBuffer data;

    public Value(final long ts, final ByteBuffer data) {
        this.ts = ts;
        this.data = data;
    }

    public static Value of(final ByteBuffer data) {
        return new Value(System.nanoTime(), data.duplicate());
    }

    public static Value tombstone(final long ttl) {
        final long now = System.nanoTime();
        return new Value(now + ttl * MILLI_TO_NANO, null);
    }

    public static Value tombstone() {
        return new Value(System.nanoTime(), null);
    }

    public boolean isRemoved() {
        return data == null;
    }

    /**
     * Returns data.
     *
     * @return data
     */
    public ByteBuffer getData() {
        if (data == null) {
            return null;
        }
        return data.asReadOnlyBuffer();
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return -Long.compare(ts, o.ts);
    }

    public long getTimeStamp() {
        return ts;
    }
}
