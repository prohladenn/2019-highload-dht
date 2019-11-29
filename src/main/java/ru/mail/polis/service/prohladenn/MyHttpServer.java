package ru.mail.polis.service.prohladenn;

import com.google.common.base.Charsets;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;

import java.net.http.HttpClient;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;

import ru.mail.polis.Record;
import ru.mail.polis.prohladenn.LSMDao;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

public class MyHttpServer extends HttpServer implements Service {

    public static final String PROXY_HEADER = "X-OK-Proxy: True";
    public static final String PROXY_HEADER_DEFAULT = "X-OK-Proxy";
    public static final String PROXY_HEADER_VALUE = "True";
    public static final String TIMESTAMP_HEADER = "X-OK-Timestamp: ";
    public static final String TIMESTAMP_HEADER_DEFAULT = "X-OK-Timestamp";
    public static final String URL = "/v0/entity?id=";

    private static final Logger logger = LoggerFactory.getLogger(MyHttpServer.class);
    @NotNull
    private final LSMDao dao;

    @NotNull
    private final Executor executor;

    @NotNull
    private final Topology<String> replicas;

    @NotNull
    private final ReplicaFactor defaultRF;

    @NotNull
    private final HttpServerController controller;

    /**
     * Create new instance of Service.
     *
     * @param port     port of service
     * @param dao      dao
     * @param executor executor
     */
    public MyHttpServer(
            final int port,
            @NotNull final DAO dao,
            @NotNull final Executor executor,
            @NotNull final Topology<String> replicas) throws IOException {
        super(from(port));
        this.dao = (LSMDao) dao;
        this.executor = executor;
        this.defaultRF = new ReplicaFactor(replicas.all().size() / 2 + 1, replicas.all().size());
        this.replicas = replicas;

        final Map<String, HttpClient> pool = new HashMap<>();
        for (final String node : this.replicas.all()) {
            pool.put(node, HttpClient.newBuilder().build());
        }
        controller = new HttpServerController(this.dao, pool, this.replicas, this.executor);
    }

    private static HttpServerConfig from(final int port) {
        final AcceptorConfig ac = new AcceptorConfig();
        ac.port = port;
        ac.reusePort = true;
        ac.deferAccept = true;

        final HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ac};
        return config;
    }

    @Override
    public HttpSession createSession(@NotNull final Socket socket) {
        return new StorageSession(socket, this);
    }

    @Path("/v0/status")
    public Response status() {
        return new Response(Response.OK, Response.EMPTY);
    }

    /**
     * Main worker method.
     *
     * @param id       id
     * @param replicas count of replicas
     * @param request  http request
     * @param session  http session
     */
    @Path("/v0/entity")
    public void entity(
            @Param("id") final String id,
            @Param("replicas") final String replicas,
            @NotNull final Request request,
            @NotNull final HttpSession session) {
        if (id == null || id.isEmpty()) {
            sendResponse(session, new Response(Response.BAD_REQUEST, "No ID".getBytes(Charset.defaultCharset())));
            return;
        }
        final ReplicaFactor rf;
        try {
            rf = replicas == null ? defaultRF : ReplicaFactor.of(replicas);
            if (rf.getAck() < 1 || rf.getFrom() < rf.getAck() || rf.getFrom() > this.replicas.all().size()) {
                throw new IllegalArgumentException("From is too big");
            }
        } catch (IllegalArgumentException e) {
            sendResponse(session, new Response(Response.BAD_REQUEST, "Wrong BF".getBytes(Charset.defaultCharset())));
            return;
        }
        final boolean proxied = request.getHeader(PROXY_HEADER) != null;
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                executeAsync(session, () -> controller.get(id, rf, proxied));
                break;
            case Request.METHOD_PUT:
                executeAsync(session, () -> controller.upsert(id, request.getBody(), rf, proxied));
                break;
            case Request.METHOD_DELETE:
                executeAsync(session, () -> controller.delete(id, rf, proxied));
                break;
            default:
                sendResponse(session, new Response(Response.METHOD_NOT_ALLOWED,
                        "Wrong method".getBytes(Charset.defaultCharset())));
                break;
        }
    }

    /**
     * Worker method includes limits.
     *
     * @param request http request
     * @param session http session
     */
    @Path("/v0/entities")
    public void entities(
            @NotNull final Request request,
            @NotNull final HttpSession session) {
        final String start = request.getParameter("start=");
        if (start == null || start.isEmpty()) {
            sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        final String end = request.getParameter("end=");
        if (end != null && end.isEmpty()) {
            sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        if (request.getMethod() != Request.METHOD_GET) {
            sendResponse(session, new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));
            return;
        }
        try {
            final Iterator<Record> records = dao.range(ByteBuffer.wrap(start.getBytes(Charsets.UTF_8)),
                    end == null ? null : ByteBuffer.wrap(end.getBytes(Charsets.UTF_8)));
            ((StorageSession) session).stream(records);
        } catch (IOException e) {
            sendResponse(session, new Response(Response.INTERNAL_ERROR,
                    e.getMessage().getBytes(Charset.defaultCharset())));
        }
    }

    @Override
    public void handleDefault(
            @NotNull final Request request,
            @NotNull final HttpSession session) {
        sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    private static void sendResponse(@NotNull final HttpSession session,
                                     @NotNull final Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            try {
                session.sendError(Response.INTERNAL_ERROR, "Error while send response");
            } catch (IOException ex) {
                logger.error("Error while send error", ex);
            }
        }
    }

    private void executeAsync(
            @NotNull final HttpSession session,
            @NotNull final Action action) {
        executor.execute(() -> {
            try {
                session.sendResponse(action.act());
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, "Error while send response");
                } catch (IOException ex) {
                    logger.error("Unable to create response", ex);
                }
            } catch (NoSuchElementException e) {
                try {
                    session.sendError(Response.NOT_FOUND, null);
                } catch (IOException ex) {
                    logger.error("Unable to send error", ex);
                }
            }
        });
    }

    @FunctionalInterface
    private interface Action {
        Response act();
    }

}
