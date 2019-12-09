package ru.mail.polis.service.prohladenn;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.prohladenn.Bytes;
import ru.mail.polis.prohladenn.LSMDao;
import ru.mail.polis.service.prohladenn.factors.ReplicaFactor;
import ru.mail.polis.service.prohladenn.factors.TimeToLiveFactor;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class HttpServerController {

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
     * @param dao      LSMDao
     * @param pool     clients
     * @param replicas replicas
     */
    public HttpServerController(@NotNull final LSMDao dao,
                                @NotNull final Map<String, HttpClient> pool,
                                @NotNull final Topology<String> replicas,
                                @NotNull final Executor executor) {
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
     */
    public Response get(
            @NotNull final String id,
            @NotNull final ReplicaFactor rf,
            final boolean isProxy) {
        // Proxy
        if (isProxy) {
            return HttpServerController.from(Value.get(id, dao), true);
        }
        // Initialize
        final Collection<Value> responses = new ArrayList<>(rf.getFrom());
        final Collection<CompletableFuture<Value>> futures = new ConcurrentLinkedQueue<>();
        // Async get
        replicas(Bytes.strToBB(id), rf.getFrom()).forEach(node -> {
            if (this.replicas.isMe(node)) {
                futures.add(CompletableFuture.supplyAsync(() -> Value.get(id, dao)));
            } else {
                final HttpRequest httpRequest = getHttpRequest(node, id).GET().build();
                futures.add(pool
                        .get(node)
                        .sendAsync(httpRequest, BodyHandlers.ofByteArray())
                        .thenApply(Value::getValueFromResponse));
            }
        });
        // Compliance replication factor
        if (checkAckCountAndCollectResponses(futures, rf, responses) >= rf.getAck()) {
            final Value value = responses.stream()
                    .filter(cell -> !cell.getState().equals(Value.State.ABSENT))
                    .max(Comparator.comparingLong(Value::getTimeStamp))
                    .orElseGet(Value::absent);
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
     */
    public Response delete(
            @NotNull final String id,
            @NotNull final ReplicaFactor rf,
            final boolean isProxy) {
        // Proxy
        if (isProxy) {
            dao.remove(Bytes.strToBB(id));
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }
        // Initialize
        final Collection<CompletableFuture<Integer>> futures = new ConcurrentLinkedQueue<>();
        // Async delete
        replicas(Bytes.strToBB(id), rf.getFrom()).forEach(node -> {
            if (this.replicas.isMe(node)) {
                futures.add(CompletableFuture
                        .runAsync(() -> dao.remove(Bytes.strToBB(id)), executor)
                        .handle((s, t) -> checkThrowableAndGetCode(202, t)));
            } else {
                final HttpRequest httpRequest = getHttpRequest(node, id).DELETE().build();
                futures.add(pool
                        .get(node)
                        .sendAsync(httpRequest, BodyHandlers.discarding())
                        .handle((a, exp) -> a.statusCode()));
            }
        });
        // Compliance replication factor
        return checkAckCountAndCreateResponse(futures, rf, Response.ACCEPTED);
    }

    /**
     * Returns status of upsert request.
     *
     * @param id      key
     * @param rf      replication factor
     * @param isProxy is proxy
     * @return response
     */
    public Response upsert(
            @NotNull final String id,
            @NotNull final byte[] value,
            @NotNull final ReplicaFactor rf,
            @NotNull final TimeToLiveFactor ttlf,
            final boolean isProxy) {
        // Proxy
        if (isProxy) {
            dao.upsert(Bytes.strToBB(id), ByteBuffer.wrap(value));
            if (ttlf != TimeToLiveFactor.EMPTY) {
                dao.timeToLive(Bytes.strToBB(id), ttlf.getTtl());
            }
            return new Response(Response.CREATED, Response.EMPTY);
        }
        // Initialize
        final Collection<CompletableFuture<Integer>> futures = new ConcurrentLinkedQueue<>();
        // Async upsert
        replicas(Bytes.strToBB(id), rf.getFrom()).forEach(node -> {
            if (this.replicas.isMe(node)) {
                futures.add(CompletableFuture
                        .runAsync(() -> {
                            dao.upsert(Bytes.strToBB(id), ByteBuffer.wrap(value));
                            if (ttlf != TimeToLiveFactor.EMPTY) {
                                dao.timeToLive(Bytes.strToBB(id), ttlf.getTtl());
                            }
                        }, executor)
                        .handle((s, t) -> checkThrowableAndGetCode(201, t)));
            } else {
                futures.add(pool
                        .get(node)
                        .sendAsync(
                                getHttpRequest(node, id).PUT(BodyPublishers.ofByteArray(value)).build(),
                                BodyHandlers.discarding())
                        .handle((a, exp) -> a.statusCode()));
            }
        });
        // Compliance replication factor
        return checkAckCountAndCreateResponse(futures, rf, Response.CREATED);
    }

    private int checkThrowableAndGetCode(final int positiveCode, final Throwable throwable) {
        return throwable == null ? positiveCode : -1;
    }

    private int checkAckCountAndCollectResponses(
            final Collection<CompletableFuture<Value>> futures,
            final ReplicaFactor rf,
            final Collection<Value> responses) {
        final AtomicInteger ackCount = new AtomicInteger(0);
        final AtomicInteger ackCountElse = new AtomicInteger(0);
        futures.forEach(future -> {
            if ((rf.getAck() - ackCount.get()) > (rf.getFrom() - ackCountElse.get() - ackCount.get())) {
                return;
            }
            try {
                final Value value = future.get();
                if (value == null) {
                    ackCountElse.incrementAndGet();
                } else {
                    responses.add(value);
                    ackCount.incrementAndGet();
                }
            } catch (InterruptedException | ExecutionException e) {
                ackCountElse.incrementAndGet();
            }
        });
        return ackCount.get();
    }

    private Response checkAckCountAndCreateResponse(
            final Collection<CompletableFuture<Integer>> futures,
            final ReplicaFactor rf,
            final String response) {
        final AtomicInteger ackCount = new AtomicInteger(0);
        final AtomicInteger ackCountElse = new AtomicInteger(0);
        futures.forEach(future -> {
            if ((rf.getAck() - ackCount.get()) > (rf.getFrom() - ackCountElse.get() - ackCount.get())) {
                return;
            }
            try {
                if (future.get() == Integer.parseInt(response.substring(0, 3))) {
                    ackCount.incrementAndGet();
                } else {
                    ackCountElse.incrementAndGet();
                }
            } catch (InterruptedException | ExecutionException e) {
                ackCountElse.incrementAndGet();
            }
        });
        if (ackCount.get() >= rf.getAck()) {
            return new Response(response, Response.EMPTY);
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    private HttpRequest.Builder getHttpRequest(final String node, final String id) {
        return HttpRequest.newBuilder()
                .uri(URI.create(node + MyHttpServer.URL + id))
                .setHeader(MyHttpServer.PROXY_HEADER_DEFAULT, MyHttpServer.PROXY_HEADER_VALUE)
                .timeout(Duration.ofSeconds(1));
    }

    private List<String> replicas(final ByteBuffer id, final int count) {
        final String[] nodes = this.replicas.all().toArray(new String[0]);
        if (count > nodes.length) {
            throw new IllegalArgumentException("Wrong input data");
        }
        int index = this.replicas.indexPrimaryFor(id);
        final String[] result = new String[count];
        for (int j = 0; j < count; j++) {
            result[j] = nodes[index];
            index = (index + 1) % nodes.length;
        }
        return List.of(result);
    }
}
