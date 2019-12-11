package ru.mail.polis.prohladenn;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Iterator;

public interface Table {
    long sizeInBytes();

    @NotNull
    Iterator<Cell> iterator(@NotNull ByteBuffer from);

    void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value);

    void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value, @NotNull Duration ttl);

    boolean contains(@NotNull ByteBuffer key);

    void remove(@NotNull ByteBuffer key);
}
