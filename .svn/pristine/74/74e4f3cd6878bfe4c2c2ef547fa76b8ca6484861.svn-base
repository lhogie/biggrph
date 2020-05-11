package biggrph.function;

import java.io.Serializable;

import biggrph.BigGraph;
import biggrph.BigGraphLong;

/**
 * A functional interface used to represent function and lambdas that process a vertex in a graph
 * and return nothing. These functions take three arguments: a {@link BigGraph}, a vertex id
 * and the attribute of the vertex as two long integers.
 *  
 * @author Nicolas Chleq
 *
 * @see BigGraphLong#forEachVertex(VertexLongConsumer)
 */
@FunctionalInterface
public interface VertexLongConsumer extends Serializable
{
	void accept(BigGraphLong gr, long v, long attr);
}
