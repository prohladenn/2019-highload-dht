package ru.mail.polis.dao;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.mail.polis.Record;
import ru.mail.polis.TestBase;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TimeToLiveTest extends TestBase {

    private static final long SECOND = 1000L;

    @Test
    void upsert(@TempDir File data) throws IOException {
        final ByteBuffer key = randomKeyBuffer();
        final ByteBuffer value = randomValueBuffer();
        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(key, value);
            dao.timeToLive(key, SECOND);
            assertEquals(value, dao.get(key));
            assertEquals(value, dao.get(key.duplicate()));
            testWait(SECOND);
            assertThrows(NoSuchElementException.class, () -> dao.get(key));
        }
    }

    @Test
    void update(@TempDir File data) throws IOException {
        final ByteBuffer key = randomKeyBuffer();
        final ByteBuffer value1 = randomValueBuffer();
        final ByteBuffer value2 = randomValueBuffer();
        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(key, value1);
            dao.timeToLive(key, SECOND);
            assertEquals(value1, dao.get(key));
            assertEquals(value1, dao.get(key.duplicate()));
            dao.upsert(key, value2);
            assertEquals(value2, dao.get(key));
            assertEquals(value2, dao.get(key.duplicate()));
            testWait(SECOND);
            assertEquals(value2, dao.get(key));
            assertEquals(value2, dao.get(key.duplicate()));
        }
    }

    @Test
    void emptyValue(@TempDir File data) throws IOException {
        final ByteBuffer key = randomKeyBuffer();
        final ByteBuffer value = ByteBuffer.allocate(0);
        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(key, value);
            dao.timeToLive(key, SECOND);
            assertEquals(value, dao.get(key));
            assertEquals(value, dao.get(key.duplicate()));
            testWait(SECOND);
            assertThrows(NoSuchElementException.class, () -> dao.get(key));
        }
    }

    @Test
    void remove(@TempDir File data) throws IOException {
        final ByteBuffer key = randomKeyBuffer();
        final ByteBuffer value = randomValueBuffer();
        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(key, value);
            dao.timeToLive(key, SECOND);
            assertEquals(value, dao.get(key));
            assertEquals(value, dao.get(key.duplicate()));
            dao.remove(key);
            assertThrows(NoSuchElementException.class, () -> dao.get(key));
            testWait(SECOND);
            assertThrows(NoSuchElementException.class, () -> dao.get(key));
        }
    }

    @Test
    void rewrite(@TempDir File data) throws IOException {
        final ByteBuffer key = randomKeyBuffer();
        final ByteBuffer value1 = randomValueBuffer();
        final ByteBuffer value2 = randomValueBuffer();
        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(key, value1);
            dao.timeToLive(key, SECOND * 5);
            assertEquals(value1, dao.get(key));
            assertEquals(value1, dao.get(key.duplicate()));
            dao.upsert(key, value2);
            dao.timeToLive(key, SECOND);
            assertEquals(value2, dao.get(key));
            assertEquals(value2, dao.get(key.duplicate()));
            testWait(SECOND);
            assertThrows(NoSuchElementException.class, () -> dao.get(key));
        }
    }

    @Test
    void manyCell(@TempDir File data) throws IOException {
        final RecordsGenerator records = new RecordsGenerator(100_000, 1);
        Record first = records.next();
        final ByteBuffer key = first.getKey();
        final ByteBuffer value = first.getValue();

        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(key, value);
            dao.timeToLive(key, SECOND * 2);
            while (records.hasNext()) {
                final Record record = records.next();
                dao.upsert(record.getKey(), record.getValue());
            }
            assertEquals(value, dao.get(key));
            assertEquals(value, dao.get(key.duplicate()));
            testWait(SECOND * 2);
            assertThrows(NoSuchElementException.class, () -> dao.get(key));
        }
    }

    @Test
    void compact(@TempDir File data) throws IOException {
        final RecordsGenerator records = new RecordsGenerator(100_000, 1);
        Record first = records.next();
        final ByteBuffer key = first.getKey();
        final ByteBuffer value = first.getValue();

        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(key, value);
            dao.timeToLive(key, SECOND * 2);
            while (records.hasNext()) {
                final Record record = records.next();
                dao.upsert(record.getKey(), record.getValue());
            }
            assertEquals(value, dao.get(key));
            assertEquals(value, dao.get(key.duplicate()));
            dao.compact();
            testWait(SECOND * 2);
            assertThrows(NoSuchElementException.class, () -> dao.get(key));
        }
    }

    @Test
    void manyCellWithTtl(@TempDir File data) throws IOException {
        final RecordsGenerator records = new RecordsGenerator(100_000, 1);
        Record first = records.next();
        final ByteBuffer key = first.getKey();
        final ByteBuffer value = first.getValue();

        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(key, value);
            dao.timeToLive(key, SECOND * 2);
            while (records.hasNext()) {
                final Record record = records.next();
                dao.upsert(record.getKey(), record.getValue());
                dao.timeToLive(record.getKey(), SECOND);
            }
            assertEquals(value, dao.get(key));
            assertEquals(value, dao.get(key.duplicate()));
            testWait(SECOND * 2);
            assertThrows(NoSuchElementException.class, () -> dao.get(key));
        }
    }

    private void testWait(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
