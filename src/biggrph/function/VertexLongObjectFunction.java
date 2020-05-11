package biggrph.function;

import java.io.Serializable;

import biggrph.BigGraphLong;

/**
 * Represents a function used to process one vertex in a graph and returns an object. The function
 * takes three arguments: a {@link BigGraphLong}, a long integer (the vertex id) and the attribute of
 * the vertex. It returns an instance of the class R.
 * 
 * @author Nicolas Chleq
 *
 * @param <R> The type of the object returned by the function
 */

@FunctionalInterface
public interface VertexLongObjectFunction<R extends Serializable> extends Serializable
{
	R apply(BigGraphLong graph, long vertexId, long attr);
}
