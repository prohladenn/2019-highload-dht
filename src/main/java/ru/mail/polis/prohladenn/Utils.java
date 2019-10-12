package ru.mail.polis.prohladenn;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.Iters;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.*;

final class Utils {
    static final ByteBuffer LEAST_KEY = ByteBuffer.allocate(0);

    private static final String TMP = ".tmp";

    private Utils() {
    }

    static Table compactFiles(@NotNull final File rootDir,
                              @NotNull final NavigableMap<Integer, Table> fileTables) throws IOException {
        final List<Iterator<Cell>> tableIterators = new LinkedList<>();
        for (final Table fileT : fileTables.values()) {
            tableIterators.add(fileT.iterator(LEAST_KEY));
        }
        final Iterator<Cell> filteredRow = getActualRowIterator(tableIterators);
        final File compactFileTmp = compact(rootDir, filteredRow);
        for (final Map.Entry<Integer, Table> entry :
                fileTables.entrySet()) {
            entry.getValue().clear();
        }
        final String fileDbName = ThreadSafeDAO.PREFIX + ThreadSafeDAO.SUFFIX;
        final File compactFileDb = new File(rootDir, fileDbName);
        Files.move(compactFileTmp.toPath(), compactFileDb.toPath(), StandardCopyOption.ATOMIC_MOVE);
        return new FileTable(compactFileDb);
    }

    private static File compact(@NotNull final File rootDir,
                                @NotNull final Iterator<Cell> rows) throws IOException {
        final String fileTableName = ThreadSafeDAO.PREFIX + TMP;
        final File table = new File(rootDir, fileTableName);
        Utils.write(table, rows);
        return table;
    }

    static void write(@NotNull final File to,
                      @NotNull final Iterator<Cell> rows) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(to.toPath(),
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            final List<Integer> offsets = writeRows(fileChannel, rows);
            writeOffsets(fileChannel, offsets);
            fileChannel.write(Bytes.fromInt(offsets.size()));
        }
    }

    private static int writeByteBuffer(@NotNull final FileChannel fc,
                                       @NotNull final ByteBuffer buffer) throws IOException {
        int offset = 0;
        offset += fc.write(Bytes.fromInt(buffer.remaining()));
        offset += fc.write(buffer);
        return offset;
    }

    private static void writeOffsets(@NotNull final FileChannel fc,
                                     @NotNull final List<Integer> offsets) throws IOException {
        for (final Integer elemOffSets : offsets) {
            fc.write(Bytes.fromInt(elemOffSets));
        }
    }

    private static List<Integer> writeRows(@NotNull final FileChannel fc,
                                           @NotNull final Iterator<Cell> rows) throws IOException {
        final List<Integer> offsets = new ArrayList<>();
        int offset = 0;
        while (rows.hasNext()) {
            offsets.add(offset);
            final Cell cell = rows.next();

            offset += writeByteBuffer(fc, cell.getKey());

            if (cell.isDead()) {
                offset += fc.write(Bytes.fromInt(ThreadSafeDAO.DEAD));
            } else {
                offset += fc.write(Bytes.fromInt(ThreadSafeDAO.ALIVE));
                offset += writeByteBuffer(fc, cell.getValue());
            }
        }
        return offsets;
    }

    static Iterator<Cell> getActualRowIterator(@NotNull final Collection<Iterator<Cell>> tableIterators) {
        final Iterator<Cell> mergingTableIterator = Iterators.mergeSorted(tableIterators, Cell::compareTo);
        final Iterator<Cell> collapsedIterator = Iters.collapseEquals(mergingTableIterator, Cell::getKey);
        return Iterators.filter(collapsedIterator, row -> !row.isDead());
    }

    static List<Iterator<Cell>> getListIterators(@NotNull final NavigableMap<Integer, Table> tables,
                                                 @NotNull final Table memTable,
                                                 @NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> tableIterators = new LinkedList<>();
        for (final Table fileT : tables.descendingMap().values()) {
            tableIterators.add(fileT.iterator(from));
        }
        final Iterator<Cell> memTableIterator = memTable.iterator(from);
        tableIterators.add(memTableIterator);
        return tableIterators;
    }
}
