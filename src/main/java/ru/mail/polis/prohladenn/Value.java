package ru.mail.polis.prohladenn;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Value implements Comparable<Value> {
    private final long ts;
    private final ByteBuffer data;

    public Value(final long ts, final ByteBuffer data) {
        this.ts = ts;
        this.data = data;
    }

    public static Value of(final ByteBuffer data) {
        return new Value(System.nanoTime(), data.duplicate());
    }

    public static Value tombstone() {
        return new Value(System.nanoTime(), null);
    }

    public boolean isRemoved() {
        return data == null;
    }

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
