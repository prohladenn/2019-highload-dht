package ru.mail.polis.service.prohladenn;

import com.google.common.base.Splitter;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class ReplicaFactor {
    private final int ack;
    private final int from;

    public ReplicaFactor(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    /**
     * Gets replication factor of value.
     *
     * @param value value
     * @return replication factor
     */
    @NotNull
    public static ReplicaFactor of(@NotNull final String value) {
        final List<String> values = Splitter.on('/').splitToList(value);
        if (values.size() != 2) {
            throw new IllegalArgumentException("Wrong RF: " + value);
        }
        return new ReplicaFactor(Integer.parseInt(values.get(0)), Integer.parseInt(values.get(1)));
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }
}
