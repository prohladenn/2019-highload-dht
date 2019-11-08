package ru.mail.polis.service.prohladenn;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.prohladenn.LSMDao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;

public class HttpServerController {

    private static final Logger logger = LoggerFactory.getLogger(MyHttpServer.class);
    private final String me;

    @NotNull
    private final LSMDao dao;

    @NotNull
    private final Map<String, HttpClient> pool;

    @NotNull
    private final Topology<String> replicas;

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
                                @NotNull final Topology<String> replicas) {
        this.me = me;
        this.dao = dao;
        this.pool = pool;
        this.replicas = replicas;
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
                return HttpServerController.from(Value.get(id.getBytes(Charset.defaultCharset()), dao), isProxy);
            } catch (IOException e) {
                return new Response(Response.INTERNAL_ERROR, e.toString().getBytes(Charset.defaultCharset()));
            }
        }
        final String[] nodes = replicas(ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8)), rf.getFrom());
        final Collection<Value> responses = new ArrayList<>(rf.getFrom());
        int ackCount = 0;
        for (final String node : nodes) {
            if (this.me.equals(node)) {
                try {
                    responses.add(Value.get(id.getBytes(Charset.defaultCharset()), dao));
                    ackCount++;
                } catch (IOException e) {
                    logger.error("Cant get {}", id, e);
                }
            } else {
                try {
                    final Response response =
                            pool.get(node).get(
                                    MyHttpServer.URL + id,
                                    MyHttpServer.PROXY_HEADER
                            );
                    ackCount++;
                    responses.add(Value.from(response));
                } catch (InterruptedException | PoolException | HttpException | IOException e) {
                    logger.error("Can't get {} from {}", id, node, e);
                }
            }
        }
        if (ackCount >= rf.getAck()) {
            return HttpServerController.from(Value.merge(responses), false);
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
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
        int ackCount = 0;
        for (final String node : nodes) {
            if (this.me.equals(node)) {
                dao.remove(ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8)));
                ackCount++;
            } else {
                try {
                    final Response response =
                            pool.get(node).delete(
                                    MyHttpServer.URL + id,
                                    MyHttpServer.PROXY_HEADER
                            );
                    if (response.getStatus() == 202) {
                        ackCount++;
                    }
                } catch (InterruptedException | PoolException | HttpException | IOException e) {
                    logger.error("Can't delete {} from {}", id, node, e);
                }
            }
        }
        if (ackCount >= rf.getAck()) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
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
        final String[] nodes = replicas(ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8)), rf.getFrom());
        int ackCount = 0;
        for (final String node : nodes) {
            if (this.me.equals(node)) {
                dao.upsert(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())), ByteBuffer.wrap(value));
                ackCount++;
            } else {
                try {
                    final Response response =
                            pool.get(node).put(
                                    MyHttpServer.URL + id,
                                    value, MyHttpServer.PROXY_HEADER
                            );
                    if (response.getStatus() == 201) {
                        ackCount++;
                    }
                } catch (InterruptedException | PoolException | HttpException | IOException e) {
                    logger.error("Can't upsert {}={} to {}", id, value, node, e);
                }
            }
        }
        if (ackCount >= rf.getAck()) {
            return new Response(Response.CREATED, Response.EMPTY);
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
