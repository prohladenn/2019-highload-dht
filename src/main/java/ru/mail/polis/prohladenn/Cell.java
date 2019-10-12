package ru.mail.polis.prohladenn;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;

import static ru.mail.polis.prohladenn.ThreadSafeDAO.DEAD;
import static ru.mail.polis.prohladenn.ThreadSafeDAO.TOMBSTONE;

final class Cell implements Comparable<Cell> {
    private final int index;
    private final int status;
    private final ByteBuffer key;
    private final ByteBuffer value;

    private Cell(final int index,
                 @NotNull final ByteBuffer key,
                 @NotNull final ByteBuffer value,
                 final int status) {
        this.index = index;
        this.key = key;
        this.value = value;
        this.status = status;
    }

    public static Cell of(final int index,
                          @NotNull final ByteBuffer key,
                          @NotNull final ByteBuffer value,
                          final int status) {
        return new Cell(index, key, value, status);
    }

    Record getRecord() {
        if (isDead()) {
            return Record.of(key, TOMBSTONE);
        } else {
            return Record.of(key, value);
        }
    }

    boolean isDead() {
        return status == DEAD;
    }

    ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    ByteBuffer getValue() {
        return value.asReadOnlyBuffer();
    }

    private int getIndex() {
        return index;
    }

    @Override
    public int compareTo(@NotNull final Cell o) {
        if (key.compareTo(o.getKey()) == 0) {
            return -Integer.compare(index, o.getIndex());
        }
        return key.compareTo(o.getKey());
    }
}
