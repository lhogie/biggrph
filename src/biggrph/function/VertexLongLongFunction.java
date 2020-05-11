package biggrph.function;

import java.io.Serializable;

import biggrph.BigGraphLong;

/**
 * Represents a function that takes 3 arguments: a graph, a vertex id and its attribute and returns a
 * long integer.
 * 
 * @author Nicolas Chleq
 *
 * @see BigGraphLong#mapVertices2Long(VertexLongLongFunction)
 */

@FunctionalInterface
public interface VertexLongLongFunction extends Serializable
{
	long apply(BigGraphLong graph, long vertexId, long attr);
}
