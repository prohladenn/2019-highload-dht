package ru.mail.polis.prohladenn;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Table {
    long sizeInBytes();

    @NotNull
    Iterator<Cell> iterator(@NotNull ByteBuffer from);

    /**
     * Upsert or update value by key.
     *
     * @param key   key
     * @param value value
     * @return if map contains key return true.
     */
    boolean upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value);

    void timeToLive(@NotNull ByteBuffer key, long ttl);

    void remove(@NotNull ByteBuffer key);
}
