package biggrph.function;

import java.io.Serializable;

import biggrph.BigGraph;

/**
 * Represents a predicate that take 3 arguments and returns a boolean. The three arguments
 * are the graph itself (an instance of {@link BigGraph}), the vertex id as a long number,
 * and the current attribute of the vertex in the graph.
 * 
 * @author Nicolas Chleq
 *
 * @param <V> the type of the vertex attribute
 * @see BigGraph#filter(VertexPredicate)
 */
@FunctionalInterface
public interface VertexPredicate<V extends Serializable> extends Serializable
{
	boolean test(BigGraph<V, ?> graph, long vertexId, V attr);
	
    default VertexPredicate<V> negate()
    {
        return (BigGraph<V, ?> g, long v, V a) -> ! test(g, v, a);
    }

    default VertexPredicate<V> and(VertexPredicate<V> other)
    {
    	return (BigGraph<V, ?> g, long v, V a) -> test(g, v, a) && other.test(g, v, a);
    }
    
    default VertexPredicate<V> or(VertexPredicate<V> other)
    {
    	return (BigGraph<V, ?> g, long v, V a) -> test(g, v, a) || other.test(g, v, a);
    }
}
