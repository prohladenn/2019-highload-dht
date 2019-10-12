package ru.mail.polis.prohladenn;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

class FileTable implements Table {
    private final int size;
    private final int count;
    private final ByteBuffer cells;
    private final IntBuffer offsets;
    private final File file;

    FileTable(@NotNull final File file) throws IOException {
        this.file = file;
        this.count = Integer.parseInt(file
                .getName()
                .substring(ThreadSafeDAO.PREFIX.length(), file.getName().length() - ThreadSafeDAO.SUFFIX.length()));
        try (FileChannel fc = FileChannel.open(file.toPath(),
                StandardOpenOption.READ)) {
            final var mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size())
                    .order(ByteOrder.BIG_ENDIAN);
            this.size = mapped.getInt(mapped.limit() - Integer.BYTES);
            final var offsetsBuffer = mapped.duplicate()
                    .position(mapped.limit() - Integer.BYTES - Integer.BYTES * size)
                    .limit(mapped.limit() - Integer.BYTES);
            this.offsets = offsetsBuffer.slice().asIntBuffer();

            this.cells = mapped.asReadOnlyBuffer()
                    .limit(offsetsBuffer.position())
                    .slice();
        }
    }

    @NotNull
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return new Iterator<>() {
            int index = getOffsetsIndex(from);

            @Override
            public boolean hasNext() {
                return index < size;
            }

            @Override
            public Cell next() {
                assert hasNext();
                return getRowAt(index++);
            }
        };
    }

    @Override
    public void clear() {
        try {
            Files.delete(file.toPath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long sizeInBytes() {
        return file.length();
    }

    private int getOffsetsIndex(@NotNull final ByteBuffer from) {
        int left = 0;
        int right = size - 1;
        while (left <= right) {
            final int middle = left + (right - left) / 2;
            final int resCmp = from.compareTo(getKeyAt(middle));
            if (resCmp < 0) {
                right = middle - 1;
            } else if (resCmp > 0) {
                left = middle + 1;
            } else {
                return middle;
            }
        }
        return left;
    }

    private ByteBuffer getKeyAt(final int i) {
        assert 0 <= i && i < size;
        final int offset = offsets.get(i);
        final int keySize = cells.getInt(offset);
        return cells.duplicate().position(offset + Integer.BYTES).limit(offset + Integer.BYTES + keySize).slice();
    }

    private Cell getRowAt(final int i) {
        assert 0 <= i && i < size;
        int offset = offsets.get(i);

        final ByteBuffer keyBB = getKeyAt(i);
        offset += Integer.BYTES + keyBB.remaining();

        final int status = cells.getInt(offset);
        offset += Integer.BYTES;

        if (status == ThreadSafeDAO.DEAD) {
            return Cell.of(count, keyBB, ThreadSafeDAO.TOMBSTONE, status);
        } else {
            final int valueSize = cells.getInt(offset);
            final ByteBuffer valueBB = cells.duplicate().position(offset + Integer.BYTES)
                    .limit(offset + Integer.BYTES + valueSize)
                    .slice();
            return Cell.of(count, keyBB, valueBB, status);
        }
    }
}
