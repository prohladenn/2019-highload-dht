package ru.mail.polis.prohladenn;

import java.util.Iterator;

public class TableToFlush {
    private final long generation;
    private final Iterator<Cell> cells;
    private final boolean poisonPill;
    private final boolean isCompacted;

    public TableToFlush(final long generation, final Iterator<Cell> cells, final boolean isCompacted) {
        this(generation, cells, false, isCompacted);
    }

    /**
     * Table that need to be flushed.
     *
     * @param generation  generation of table
     * @param cells       data
     * @param poisonPill  indicator
     * @param isCompacted if table needs to be compact
     */
    public TableToFlush(final long generation, final Iterator<Cell> cells, final boolean poisonPill,
                        final boolean isCompacted) {
        this.generation = generation;
        this.cells = cells;
        this.isCompacted = isCompacted;
        this.poisonPill = poisonPill;
    }

    public long getGeneration() {
        return generation;
    }

    public Iterator<Cell> getData() {
        return cells;
    }

    public boolean isPoisonPill() {
        return poisonPill;
    }

    public boolean isCompacted() {
        return isCompacted;
    }
}
