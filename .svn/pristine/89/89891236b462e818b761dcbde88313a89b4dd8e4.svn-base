package biggrph.function;

import java.io.Serializable;

import biggrph.BigGraph;

/**
 * A functional interface used to represent function and lambdas that process a vertex in a graph
 * and return nothing. These functions take three arguments: a {@link BigGraph}, a vertex id
 * as a long integer and the attribute of the vertex.
 *  
 * @author Nicolas Chleq
 *
 * @param <V> the type of the vertex attributes of the graph.
 * @see BigGraph#forEachVertex(VertexConsumer)
 */
@FunctionalInterface
public interface VertexConsumer<V extends Serializable> extends Serializable
{
    void accept(BigGraph<V, ?> gr, long v, V attr);
}
