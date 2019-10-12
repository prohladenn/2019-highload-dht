package ru.mail.polis.prohladenn;

import org.jetbrains.annotations.NotNull;

class TableToFlush {
    private final Table table;
    private final int fileIndex;
    private final boolean poisonPill;
    private final boolean compacting;

    TableToFlush(@NotNull final Table table, final int fileIndex) {
        this(table, fileIndex, false);
    }

    TableToFlush(@NotNull final Table table, final int fileIndex, final boolean poisonPill) {
        this(table, fileIndex, poisonPill, false);
    }

    TableToFlush(@NotNull final Table table, final int fileIndex, final boolean poisonPill, final boolean compacting) {
        this.table = table;
        this.fileIndex = fileIndex;
        this.poisonPill = poisonPill;
        this.compacting = compacting;
    }

    Table getTable() {
        return table;
    }

    int getFileIndex() {
        return fileIndex;
    }

    boolean isPoisonPill() {
        return poisonPill;
    }

    boolean isCompacting() {
        return compacting;
    }
}
