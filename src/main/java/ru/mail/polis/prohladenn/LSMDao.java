package ru.mail.polis.prohladenn;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class LSMDao implements DAO {
    public static final ByteBuffer EMPTY = ByteBuffer.allocate(0);
    public static final String TABLE = "FILE_TABLE";
    public static final String SUFFIX = ".db";
    public static final String TEMP = ".tmp";
    private static final Logger log = LoggerFactory.getLogger(ru.mail.polis.prohladenn.LSMDao.class);

    private final File base;
    private Collection<FileTable> fileTables;
    private final MemTablePool memTable;
    private final Thread flushedThread;
    private final long generation;

    private final class FlusherThread extends Thread {

        public FlusherThread() {
            super("flusher");
        }

        @Override
        public void run() {
            boolean poisonRecieved = false;
            while (!Thread.currentThread().isInterrupted() && !poisonRecieved) {
                TableToFlush toFlush;
                try {
                    toFlush = memTable.takeToFlush();
                    final Iterator<Cell> data = toFlush.getData();
                    poisonRecieved = toFlush.isPoisonPill();
                    final boolean isCompactTable = toFlush.isCompacted();
                    if (isCompactTable || poisonRecieved) {
                        flush(toFlush.getGeneration(), true, data);
                    } else {
                        flush(toFlush.getGeneration(), false, data);
                    }
                    if (isCompactTable) {
                        compactDir(toFlush.getGeneration());
                    } else {
                        memTable.flushed(toFlush.getGeneration());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    log.info("Error while flush generation :" + e.getMessage());
                }
            }
            if (poisonRecieved) {
                log.info("Poison pill received. Stop flushing.");
            }
        }
    }

    /**
     * Creates persistence LSMDao.
     *
     * @param base           folder with FileTable
     * @param flushThreshold threshold memTable's size
     * @throws IOException if an I/O error occurred
     */
    public LSMDao(
            final File base,
            final long flushThreshold) throws IOException {
        assert flushThreshold >= 0L;
        this.base = base;
        this.fileTables = new ArrayList<>();
        final AtomicLong maxGeneration = new AtomicLong();
        Files.walkFileTree(base.toPath(), EnumSet.of(FileVisitOption.FOLLOW_LINKS), 1, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path path, final BasicFileAttributes attrs) {
                if (!path.getFileName().toString().contains("trash")) {
                    try {
                        maxGeneration.set(Math.max(maxGeneration.get(), getGeneration(path.toFile())));
                        fileTables.add(new FileTable(path.toFile()));
                    } catch (IOException e) {
                        log.error(e.getMessage());
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
        this.generation = maxGeneration.get() + 1;
        this.memTable = new MemTablePool(generation, flushThreshold);
        flushedThread = new FlusherThread();
        flushedThread.start();
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        return Iterators.transform(
                cellIterator(from),
                cell -> Record.of(cell.getKey(), cell.getValue().getData()));
    }

    @NotNull
    private Iterator<Cell> cellIterator(@NotNull final ByteBuffer from) throws IOException {
        return IterUtils.collapse(memTable, fileTables, from);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        memTable.upsert(key, value);
    }

    private void flush(final long currentGeneration,
                       final boolean isCompactFlush,
                       @NotNull final Iterator<Cell> iterator) throws IOException {
        if (!iterator.hasNext()) return;
        final File file = new File(base, currentGeneration + TABLE + SUFFIX);
        FileTable.write(iterator, file);
        if (isCompactFlush) {
            fileTables.add(new FileTable(file));
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        memTable.remove(key);
    }

    @Override
    public void compact() throws IOException {
        memTable.compact(fileTables, generation, base);
    }

    @Override
    public void close() {
        memTable.close();
        try {
            flushedThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private long getGeneration(final File file) {
        final String[] str = file.getName().split(TABLE);
        return Long.parseLong(str[0]);
    }

    private void compactDir(final long preGender) throws IOException {
        fileTables = new ArrayList<>();
        Files.walkFileTree(base.toPath(), EnumSet.of(FileVisitOption.FOLLOW_LINKS), 1, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path path, final BasicFileAttributes attrs) throws IOException {
                final File file = path.toFile();
                final Matcher matcher = Pattern.compile(TABLE).matcher(file.getName());
                if (file.getName().endsWith(SUFFIX) && matcher.find()) {
                    final long currentGeneration = getGeneration(file);
                    if (currentGeneration >= preGender) {
                        fileTables.add(new FileTable(file));
                        return FileVisitResult.CONTINUE;
                    }
                }
                Files.delete(path);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
