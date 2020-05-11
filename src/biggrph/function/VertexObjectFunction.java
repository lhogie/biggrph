package biggrph.function;

import java.io.Serializable;

import biggrph.BigGraph;

/**
 * Represents a function used to process one vertex in a graph and returns an object. The function
 * takes three arguments: a {@link BigGraph}, a long integer (the vertex id) and the attribute of
 * the vertex. It returns an instance of the class R.
 * 
 * @author Nicolas Chleq
 *
 * @param <V> The type of the vertex attribute
 * @param <R> The type of the object returned by the function
 */
@FunctionalInterface
public interface VertexObjectFunction<V extends Serializable, R extends Serializable> extends Serializable
{
	R apply(BigGraph<V, ?> graph, long vertexId, V attr);
}
