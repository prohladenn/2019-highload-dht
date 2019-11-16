package ru.mail.polis.service.prohladenn;

import java.net.http.HttpClient;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.prohladenn.LSMDao;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class HttpServerController {

    private static final Logger logger = LoggerFactory.getLogger(HttpServerController.class);
    private final String me;

    @NotNull
    private final LSMDao dao;

    @NotNull
    private final Map<String, HttpClient> pool;

    @NotNull
    private final Topology<String> replicas;

    @NotNull
    private final Executor executor;

    /**
     * Creates instance of HttpServer controller.
     *
     * @param me       current node
     * @param dao      LSMDao
     * @param pool     clients
     * @param replicas replicas
     */
    public HttpServerController(final String me,
                                @NotNull final LSMDao dao,
                                @NotNull final Map<String, HttpClient> pool,
                                @NotNull final Topology<String> replicas,
                                @NotNull final Executor executor) {
        this.me = me;
        this.dao = dao;
        this.pool = pool;
        this.replicas = replicas;
        this.executor = executor;
    }

    /**
     * Creates response from value.
     *
     * @param value value
     * @param proxy is proxied
     * @return response of value
     */
    @NotNull
    public static Response from(
            @NotNull final Value value,
            final boolean proxy) {
        Response result;
        switch (value.getState()) {
            case PRESENT:
                result = new Response(Response.OK, value.getData());
                if (proxy) {
                    result.addHeader(MyHttpServer.TIMESTAMP_HEADER + value.getTimeStamp());
                }
                return result;
            case REMOVED:
                result = new Response(Response.NOT_FOUND, Response.EMPTY);
                if (proxy) {
                    result.addHeader(MyHttpServer.TIMESTAMP_HEADER + value.getTimeStamp());
                }
                return result;
            case ABSENT:
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            default:
                throw new IllegalArgumentException("Wrong input data");
        }
    }

    /**
     * Returns status of get request.
     *
     * @param id      key
     * @param rf      replication factor
     * @param isProxy is proxy
     * @return response
     * @throws NoSuchElementException if occurred
     */
    public Response get(
            @NotNull final String id,
            @NotNull final ReplicaFactor rf,
            final boolean isProxy) throws NoSuchElementException {
        if (isProxy) {
            try {
                return HttpServerController.from(Value.get(id.getBytes(Charset.defaultCharset()), dao), true);
            } catch (IOException e) {
                return new Response(Response.INTERNAL_ERROR, e.toString().getBytes(Charset.defaultCharset()));
            }
        }
        final String[] nodes = replicas(ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8)), rf.getFrom());
        final Collection<Value> responses = new ArrayList<>(rf.getFrom());
        final Collection<CompletableFuture<Value>> futures = new ConcurrentLinkedQueue<>();
        for (final String node : nodes) {
            if (this.me.equals(node)) {
                futures.add(CompletableFuture.supplyAsync(() ->
                {
                    try {
                        return Value.get(id.getBytes(StandardCharsets.UTF_8), dao);
                    } catch (IOException e) {
                        logger.error("Can not get", e);
                        return null;
                    }
                }));
            } else {
                final HttpRequest httpRequest = HttpRequest.newBuilder().uri(URI.create(node + MyHttpServer.URL + id))
                        .setHeader(MyHttpServer.PROXY_HEADER_DEFAULT, MyHttpServer.PROXY_HEADER_VALUE)
                        .timeout(Duration.ofSeconds(1))
                        .GET().build();
                futures.add(pool.get(node).sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray()).
                        thenApply(Value::getData));
            }
        }
        AtomicInteger ackCount = new AtomicInteger(0);
        AtomicInteger ackCountFalse = new AtomicInteger(0);
        futures.forEach(f -> {
            if ((rf.getAck() - ackCount.get()) > (rf.getFrom() - ackCountFalse.get() - ackCount.get()))
                return;
            try {
                Value value = f.get();
                if (value != null) {
                    responses.add(value);
                    ackCount.incrementAndGet();
                } else {
                    ackCountFalse.incrementAndGet();
                }
            } catch (InterruptedException | ExecutionException e) {
                ackCountFalse.incrementAndGet();
            }
        });

        if (ackCount.get() >= rf.getAck()) {
            final Value value = responses.stream().filter(Cell -> Cell.getState() != Value.State.ABSENT)
                    .max(Comparator.comparingLong(Value::getTimeStamp)).orElseGet(Value::absent);
            return from(value, false);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    /**
     * Returns status of delete request.
     *
     * @param id      key
     * @param rf      replication factor
     * @param isProxy is proxy
     * @return response
     * @throws NoSuchElementException if occurred
     */
    public Response delete(
            @NotNull final String id,
            @NotNull final ReplicaFactor rf,
            final boolean isProxy) {
        if (isProxy) {
            dao.remove(ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8)));
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }
        final String[] nodes = replicas(ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8)), rf.getFrom());
        final Collection<CompletableFuture<Integer>> futures = new ConcurrentLinkedQueue<>();
        for (final String node : nodes) {
            if (this.me.equals(node)) {
                final CompletableFuture<Integer> future = CompletableFuture.runAsync(() ->
                                dao.remove(ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8))),
                        executor).handle((s, t) -> {
                    if (t != null) {
                        return -1;
                    }
                    return 202;
                });
                futures.add(future);
            } else {
                final HttpRequest httpRequest = HttpRequest.newBuilder().DELETE()
                        .uri(URI.create(node + MyHttpServer.URL + id))
                        .setHeader(MyHttpServer.PROXY_HEADER_DEFAULT, MyHttpServer.PROXY_HEADER_VALUE)
                        .timeout(Duration.ofSeconds(1))
                        .build();
                futures.add(pool.get(node).sendAsync(httpRequest, HttpResponse.BodyHandlers.discarding()).
                        handle((a, exp) -> a.statusCode()));
            }
        }
        AtomicInteger ackCount = new AtomicInteger(0);
        AtomicInteger ackCountElse = new AtomicInteger(0);
        futures.forEach(f -> {
            if ((rf.getAck() - ackCount.get()) > (rf.getFrom() - ackCountElse.get() - ackCount.get()))
                return;
            try {
                if (f.get() == 202) {
                    ackCount.incrementAndGet();
                } else {
                    ackCountElse.incrementAndGet();
                }
            } catch (InterruptedException | ExecutionException e) {
                ackCountElse.incrementAndGet();
            }
        });
        return checkAckCount(ackCount.get(), rf, Response.ACCEPTED);
    }

    /**
     * Returns status of upsert request.
     *
     * @param id      key
     * @param rf      replication factor
     * @param isProxy is proxy
     * @return response
     * @throws NoSuchElementException if occurred
     */
    public Response upsert(
            @NotNull final String id,
            @NotNull final byte[] value,
            @NotNull final ReplicaFactor rf,
            final boolean isProxy) {
        if (isProxy) {
            dao.upsert(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())), ByteBuffer.wrap(value));
            return new Response(Response.CREATED, Response.EMPTY);
        }
        final String[] nodes = replicas(ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8)), replicas.all().size());

        final Collection<CompletableFuture<Integer>> futures = new ConcurrentLinkedQueue<>();
        for (final String node : nodes) {
            if (this.me.equals(node)) {
                final CompletableFuture<Integer> future = CompletableFuture.runAsync(() ->
                        dao.upsert(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())),
                                ByteBuffer.wrap(value)), executor).handle((s, t) -> {
                    if (t != null) {
                        return -1;
                    }
                    return 201;
                });
                futures.add(future);
            } else {
                final HttpRequest httpRequest = HttpRequest.newBuilder()
                        .uri(URI.create(node + MyHttpServer.URL + id))
                        .setHeader(MyHttpServer.PROXY_HEADER_DEFAULT, MyHttpServer.PROXY_HEADER_VALUE)
                        .timeout(Duration.ofSeconds(1))
                        .PUT(HttpRequest.BodyPublishers.ofByteArray(value))
                        .build();
                final CompletableFuture<Integer> response =
                        pool.get(node).sendAsync(httpRequest, HttpResponse.BodyHandlers.discarding()).
                                handle((a, exp) -> a.statusCode());
                futures.add(response);
            }
        }
        AtomicInteger ackCount = new AtomicInteger(0);
        AtomicInteger ackCountElse = new AtomicInteger(0);
        futures.forEach(f -> {
            if ((rf.getAck() - ackCount.get()) > (rf.getFrom() - ackCountElse.get() - ackCount.get()))
                return;
            try {
                if (f.get() == 201) {
                    ackCount.incrementAndGet();
                } else {
                    ackCountElse.incrementAndGet();
                }
            } catch (InterruptedException | ExecutionException e) {
                ackCountElse.incrementAndGet();
            }
        });

        return checkAckCount(ackCount.get(), rf, Response.CREATED);
    }

    private Response checkAckCount(final int ackCount, final ReplicaFactor rf, final String response) {
        if (ackCount >= rf.getAck()) {
            return new Response(response, Response.EMPTY);
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    private String[] replicas(final ByteBuffer id, final int count) {
        final String[] nodes = this.replicas.all().toArray(new String[0]);
        if (count > nodes.length) {
            throw new IllegalArgumentException("Wrong input data");
        }
        final String[] result = new String[count];
        int index = (id.hashCode() & Integer.MAX_VALUE) % nodes.length;
        for (int j = 0; j < count; j++) {
            result[j] = nodes[index];
            index = (index + 1) % nodes.length;
        }
        return result;
    }
}
