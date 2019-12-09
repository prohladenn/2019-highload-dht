package ru.mail.polis.service.prohladenn.factors;

import org.jetbrains.annotations.NotNull;

public class TimeToLiveFactor {
    public static final TimeToLiveFactor EMPTY = new TimeToLiveFactor(-1);

    private final long ttl;

    public TimeToLiveFactor(long ttl) {
        this.ttl = ttl;
    }

    /**
     * Gets replication factor of value.
     *
     * @param value value
     * @return replication factor
     */
    @NotNull
    public static TimeToLiveFactor of(@NotNull final String value) {
        return new TimeToLiveFactor(Long.parseLong(value));
    }

    public long getTtl() {
        return ttl;
    }
}
