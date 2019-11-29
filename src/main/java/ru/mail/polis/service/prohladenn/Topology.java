package ru.mail.polis.service.prohladenn;

import org.jetbrains.annotations.NotNull;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.Set;

@ThreadSafe
public interface Topology<T> {
    boolean isMe(@NotNull T node);

    @NotNull
    T primaryFor(@NotNull ByteBuffer key);

    int indexPrimaryFor(@NotNull ByteBuffer key);

    @NotNull
    Set<T> all();
}
