package ru.mail.polis.service.prohladenn;

import com.google.common.base.Charsets;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.Path;
import one.nio.http.Response;
import one.nio.http.Request;
import one.nio.http.HttpSession;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;

import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

public class MyService extends HttpServer implements Service {
    private static final Logger logger = LoggerFactory.getLogger(MyService.class);

    private final DAO dao;

    @NotNull
    private final Executor executor;

    /**
     * Create new instance of Service.
     *
     * @param port     port of service
     * @param dao      dao
     * @param executor executor
     */
    public MyService(
            @NotNull final int port,
            @NotNull final DAO dao,
            @NotNull final Executor executor) throws IOException {
        super(from(port));
        this.dao = dao;
        this.executor = executor;
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


    private Response get(
            @NotNull final String id) throws IOException, NoSuchElementException {
        final ByteBuffer value = dao.get(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())));
        final ByteBuffer duplicate = value.duplicate();
        final byte[] bytes = new byte[duplicate.limit()];
        duplicate.get(bytes);
        return new Response(Response.OK, bytes);
    }

    private Response delete(
            @NotNull final String id) throws IOException {
        dao.remove(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }


    private Response upsert(
            @NotNull final String id,
            @NotNull final byte[] value) throws IOException {
        dao.upsert(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())), ByteBuffer.wrap(value));
        return new Response(Response.CREATED, Response.EMPTY);
    }


    @Path("/v0/entity")
    private void entity(@NotNull final Request request,
                        @NotNull final HttpSession session) {
        final String id = request.getParameter("id=");
        if (id == null || id.isEmpty()) {
            sendResponse(session, new Response(Response.BAD_REQUEST,
                    "No id".getBytes(Charset.defaultCharset())));
            return;
        }
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                executeAsync(session, () -> get(id));
                break;
            case Request.METHOD_PUT:
                executeAsync(session, () -> upsert(id, request.getBody()));
                break;
            case Request.METHOD_DELETE:
                executeAsync(session, () -> delete(id));
                break;
            default:
                sendResponse(session, new Response(Response.METHOD_NOT_ALLOWED,
                        "Wrong method".getBytes(Charset.defaultCharset())));
                break;
        }
    }

    @Path("/v0/entities")
    private void entities(
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
        switch (request.getPath()) {
            case "/v0/entity":
                entity(request, session);
                break;
            case "/v0/entities":
                entities(request, session);
                break;
            default:
                sendResponse(session, new Response(Response.BAD_REQUEST,
                        "Wrong path".getBytes(Charset.defaultCharset())));
                break;
        }
    }

    private static void sendResponse(@NotNull final HttpSession session,
                                     @NotNull final Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            try {
                session.sendError(Response.INTERNAL_ERROR, "Error while send response");
            } catch (IOException ex) {
                logger.error("Error while send error");
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
                logger.error("Unable to create response", e);
            } catch (NoSuchElementException e) {
                try {
                    session.sendError(Response.NOT_FOUND, null);
                } catch (IOException ex) {
                    logger.error("Unable to send error");
                }
            }
        });
    }

    @FunctionalInterface
    private interface Action {
        Response act() throws IOException;
    }
}
