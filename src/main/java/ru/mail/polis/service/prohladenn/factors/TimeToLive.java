package ru.mail.polis.service.prohladenn.factors;

import org.jetbrains.annotations.NotNull;

public class TimeToLive {
    public static final TimeToLive EMPTY = new TimeToLive(-1);

    private final long ttl;

    public TimeToLive(final long ttl) {
        this.ttl = ttl;
    }

    @NotNull
    public static TimeToLive of(@NotNull final String value) {
        return new TimeToLive(Long.parseLong(value));
    }

    public long getTtl() {
        return ttl;
    }
}
