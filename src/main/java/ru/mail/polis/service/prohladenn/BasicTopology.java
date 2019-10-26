package ru.mail.polis.service.prohladenn;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

public class BasicTopology implements Topology<String> {
    @NotNull
    private final String me;
    @NotNull
    private final String[] nodes;

    /**
     * Creates new instance of basic realisation of topology.
     *
     * @param nodes all urls
     * @param me    current url
     */

    public BasicTopology(@NotNull final Set<String> nodes,
                         @NotNull final String me) {
        assert nodes.contains(me);
        this.me = me;
        this.nodes = new String[nodes.size()];
        nodes.toArray(this.nodes);
        Arrays.sort(this.nodes);
    }

    @Override
    public boolean isMe(@NotNull final String node) {
        return me.equals(node);
    }

    @NotNull
    @Override
    public String primaryFor(@NotNull final ByteBuffer key) {
        final int hash = key.hashCode();
        final int node = (hash & Integer.MAX_VALUE) % nodes.length;
        return nodes[node];
    }

    @NotNull
    @Override
    public Set<String> all() {
        return Set.of(nodes);
    }
}
