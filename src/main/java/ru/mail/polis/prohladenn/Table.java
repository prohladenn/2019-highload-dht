package ru.mail.polis.prohladenn;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public interface Table {
    @NotNull
    Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException;

    default void upsert(
            @NotNull ByteBuffer key,
            @NotNull ByteBuffer value,
            @NotNull AtomicInteger fileIndex) {
    }

    default void remove(@NotNull ByteBuffer key,
                        @NotNull AtomicInteger fileIndex) {
    }

    void clear();

    long sizeInBytes();
}
