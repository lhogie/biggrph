package biggrph.function;

import java.io.Serializable;

import biggrph.BigGraph;

/**
 * Represents a function that takes 3 arguments: a graph, a vertex id and its attribute and returns a
 * long integer.
 * 
 * @author Nicolas Chleq
 *
 * @param <V> the type of the vertex attribute
 * @see BigGraph#mapVertices2Long(VertexLongFunction)
 */
@FunctionalInterface
public interface VertexLongFunction<V extends Serializable> extends Serializable
{
	long apply(BigGraph<V, ?> graph, long vertexId, V attr);
}
