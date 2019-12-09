/*
 * Copyright 2019 (c) Odnoklassniki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mail.polis.service;

import one.nio.http.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.mail.polis.Files;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.DAOFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class TimeToLiveTest extends ClusterTestBase {
    private static final long SECOND = 1000L;
    private static final Duration TIMEOUT = Duration.ofMinutes(1);
    private File data0;
    private DAO dao0;
    private Service storage0;
    private Response response;

    @BeforeEach
    void beforeEach() throws Exception {
        int port0 = randomPort();
        endpoints = new LinkedHashSet<>(List.of(endpoint(port0)));
        data0 = Files.createTempDirectory();
        dao0 = DAOFactory.create(data0);
        storage0 = ServiceFactory.create(port0, dao0, endpoints);
        storage0.start();
    }

    @AfterEach
    void afterEach() throws IOException {
        stop(0, storage0);
        dao0.close();
        Files.recursiveDelete(data0);
        endpoints = Collections.emptySet();
    }

    @Test
    void upsert() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(0, key, value, SECOND).getStatus());

            // Wait
            testWait(SECOND);

            // Check
            response = get(0, key);
            assertEquals(404, response.getStatus());
        });
    }

    @Test
    void rewrite() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(0, key, randomValue(), SECOND).getStatus());
            assertEquals(201, upsert(0, key, value, SECOND).getStatus());

            // Wait
            testWait(SECOND);

            // Check
            response = get(0, key);
            assertEquals(404, response.getStatus());
        });
    }

    private void testWait(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
