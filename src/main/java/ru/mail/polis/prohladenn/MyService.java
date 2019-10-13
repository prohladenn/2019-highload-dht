package ru.mail.polis.prohladenn;

import com.google.common.base.Charsets;
import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public class MyService extends HttpServer implements Service {

    private final DAO dao;

    public MyService(final int port,
                     @NotNull final DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
    }

    @Path("/v0/entity")
    public Response entity(@NotNull final Request request,
                           @Param("id") final String id) {
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    final ByteBuffer value = dao.get(key);
                    final ByteBuffer duplicate = value.duplicate();
                    byte[] body = new byte[duplicate.remaining()];
                    duplicate.get(body);
                    return new Response(Response.OK, body);
                case Request.METHOD_PUT:
                    dao.upsert(key, ByteBuffer.wrap(request.getBody()));
                    return new Response(Response.CREATED, Response.EMPTY);
                case Request.METHOD_DELETE:
                    dao.remove(key);
                    return new Response(Response.ACCEPTED, Response.EMPTY);
                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
            }
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, "Key not found".getBytes(Charsets.UTF_8));
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    @Path("/v0/status")
    public Response status() {
        return new Response(Response.OK, Response.EMPTY);
    }

    @Override
    public void handleDefault(final Request request,
                              final HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    private static HttpServerConfig getConfig(final int port) {
        if (port <= 0 || 65536 <= port) {
            throw new IllegalArgumentException("Port out of range");
        }
        final AcceptorConfig acceptor = new AcceptorConfig();
        final HttpServerConfig config = new HttpServerConfig();
        acceptor.port = port;
        config.acceptors = new AcceptorConfig[]{acceptor};
        config.selectors = 4;
        return config;
    }

}
