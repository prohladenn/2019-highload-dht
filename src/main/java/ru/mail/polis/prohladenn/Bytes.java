package ru.mail.polis.prohladenn;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Bytes {
    private Bytes() {
    }

    static ByteBuffer fromInt(final int value) {
        final ByteBuffer result = ByteBuffer.allocate(Integer.BYTES);
        result.putInt(value);
        result.rewind();
        return result;
    }

    static ByteBuffer fromLong(final long value) {
        final ByteBuffer result = ByteBuffer.allocate(Long.BYTES);
        result.putLong(value);
        result.rewind();
        return result;
    }

    /**
     * Transforms ByteBuffer to array of bytes.
     *
     * @param buffer input byte buffer
     * @return byte array
     */
    public static byte[] toArray(@NotNull final ByteBuffer buffer) {
        final ByteBuffer duplicate = buffer.duplicate();
        final byte[] array = new byte[duplicate.remaining()];
        duplicate.get(array);
        return array;
    }
}
