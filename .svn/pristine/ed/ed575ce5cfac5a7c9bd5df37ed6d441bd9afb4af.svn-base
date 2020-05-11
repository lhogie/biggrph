package biggrph.function;

import java.io.Serializable;

import biggrph.BigGraphLong;

public interface VertexLongPredicate extends Serializable
{
	boolean test(BigGraphLong graph, long vertexId, long attr);
	
    default VertexLongPredicate negate()
    {
        return (BigGraphLong g, long v, long a) -> ! test(g, v, a);
    }

    default VertexLongPredicate and(VertexLongPredicate other)
    {
    	return (BigGraphLong g, long v, long a) -> test(g, v, a) && other.test(g, v, a);
    }
    
    default VertexLongPredicate or(VertexLongPredicate other)
    {
    	return (BigGraphLong g, long v, long a) -> test(g, v, a) || other.test(g, v, a);
    }

}
