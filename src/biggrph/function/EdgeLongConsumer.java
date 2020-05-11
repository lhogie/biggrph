package biggrph.function;

import java.io.Serializable;

import biggrph.BigGraphLong;

/**
 * Represents a function that processes an edge of a graph and returns nothing. The function
 * takes five arguments. The first is the graph, the second and third argument are the vertex
 * ids of the source and destination vertices of the edge, and the fourth and fifth arguments
 * are the attributes of the source and destination vertices respectively.
 * 
 * @author Nicolas Chleq
 */
@FunctionalInterface
public interface EdgeLongConsumer extends Serializable
{
	void accept(BigGraphLong gr, long src, long dst, long srcAttr, long dstAttr);
}
