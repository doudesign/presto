package com.facebook.presto.raptorx.metadata;

import com.facebook.presto.spi.Node;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class RaptorNode
{
    private final long nodeId;
    private final Node node;

    public RaptorNode(long nodeId, Node node)
    {
        this.nodeId = nodeId;
        this.node = requireNonNull(node, "node is null");
    }

    public long getNodeId()
    {
        return nodeId;
    }

    public Node getNode()
    {
        return node;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nodeId", nodeId)
                .add("identifier", node.getNodeIdentifier())
                .add("address", node.getHostAndPort())
                .toString();
    }
}
