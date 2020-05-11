package biggrph.function;

import java.io.Serializable;

import biggrph.BigGraph;

/**
 * Represents a function that processes an edge of a graph and returns nothing. The function
 * takes five arguments. The first is the graph, the second and third argument are the vertex
 * ids of the source and destination vertices of the edge, and the fourth and fifth arguments
 * are the attributes of the source and destination vertices respectively.
 * 
 * @author Nicolas Chleq
 *
 * @param <V> the type of the vertex attribute
 */
@FunctionalInterface
public interface EdgeConsumer<V extends Serializable, E extends Serializable> extends Serializable
{
    void accept(BigGraph<V, E> gr, long src, long dst, E edgeAttr);
}
