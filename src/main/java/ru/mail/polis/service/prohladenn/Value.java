package ru.mail.polis.service.prohladenn;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.prohladenn.Cell;
import ru.mail.polis.prohladenn.LSMDao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;

public final class Value implements Comparable<Value> {
    private static final Value ABSENT = new Value(-1, null, State.ABSENT);

    private final long ts;
    private byte[] data;

    public enum State {
        PRESENT,
        REMOVED,
        ABSENT
    }

    private final State state;

    Value(final long ts, final byte[] data, @NotNull final State state) {
        this.ts = ts;
        if (data != null) {
            this.data = new byte[data.length];
            System.arraycopy(data, 0, this.data, 0, data.length);
        }
        this.state = state;
    }

    @NotNull
    public State getState() {
        return state;
    }

    /**
     * Returns data.
     *
     * @return data
     */
    public byte[] getData() {
        if (data == null) {
            throw new IllegalArgumentException("Cell data is null");
        }
        return Arrays.copyOf(data, data.length);
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return -Long.compare(ts, o.ts);
    }

    public long getTimeStamp() {
        return ts;
    }

    public static Value present(
            @NotNull final byte[] data,
            final long timestamp) {
        return new Value(timestamp, data, State.PRESENT);
    }

    public static Value removed(final long ts) {
        return new Value(ts, null, State.REMOVED);
    }

    public static Value absent() {
        return ABSENT;
    }

    /**
     * Merge and get latest value.
     *
     * @param values collection
     * @return merged value of collection
     */
    @NotNull
    public static Value merge(@NotNull final Collection<Value> values) {
        return values.stream()
                .filter(value -> value.getState() != State.ABSENT)
                .max(Comparator.comparingLong(Value::getTimeStamp))
                .orElseGet(Value::absent);
    }

    /**
     * Get value from response.
     *
     * @param response response
     * @return value
     * @throws IOException if an I/O error occurred
     */
    @NotNull
    public static Value from(@NotNull final Response response) throws IOException {
        final String ts = response.getHeader(MyHttpServer.TIMESTAMP_HEADER);
        final int status = response.getStatus();
        if (status == 200) {
            if (ts == null) {
                throw new IllegalArgumentException("Wrong input data");
            }
            return Value.present(
                    response.getBody(),
                    Long.parseLong(ts)
            );
        } else if (status == 404) {
            if (ts == null) {
                return Value.absent();
            } else {
                return Value.removed(Long.parseLong(ts));
            }
        } else {
            throw new IOException("Wrong status");
        }
    }

    /**
     * Gets value from dao.
     *
     * @param key key
     * @param dao LSMDao
     * @return value from dao
     * @throws IOException if an I/O error occurred
     */
    public static Value get(final byte[] key, @NotNull final LSMDao dao) throws IOException {
        final ByteBuffer k = ByteBuffer.wrap(key);
        final Iterator<Cell> cells = dao.latestIterator(k);
        if (!cells.hasNext()) {
            return Value.absent();
        }

        final Cell cell = cells.next();
        if (!cell.getKey().equals(k)) {
            return Value.absent();
        }

        if (cell.getValue().getData() == null) {
            return Value.removed(cell.getValue().getTimeStamp());
        } else {
            final ByteBuffer v = cell.getValue().getData();
            final byte[] buf = new byte[v.remaining()];
            v.duplicate().get(buf);
            return Value.present(buf, cell.getValue().getTimeStamp());
        }
    }
}
