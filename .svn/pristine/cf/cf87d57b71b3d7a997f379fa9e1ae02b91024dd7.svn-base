package biggrph;

import java.io.Serializable;

import bigobject.Allocation;
import bigobject.BigObjectReference;
import bigobject.LiveDistributedObject;
import dht.DHT;

/**
 * Abstract base class for graphs with attributes on the vertices.
 * 
 * @author Nicolas Chleq
 *
 * @param <A> the type of the local structure used on each cluster node to store
 *            the attributes of the
 *            vertices. Must be a subclass of {@link DHT}.
 */
public abstract class AbstractBigGraph<A extends DHT<?, ?>, E extends DHT<?, ?>>
		extends LiveDistributedObject<Serializable, Serializable>
{
	private BigObjectReference<GraphTopology> topology;
	protected final static String TOPOLOGY_SUFFIX = "/topology";
	private BigObjectReference<A> vertexAttributes;
	protected final static String VERTEX_ATTR_SUFFIX = "/vAttributes";
	private BigObjectReference<E> edgeAttributes;
	protected final static String EDGE_ATTR_SUFFIX = "/eAttributes";

	protected AbstractBigGraph(String id, Allocation allocation, String topologyID, String vAttributesID, String eAttributesID)
	{
		super(id, allocation, topologyID, vAttributesID, eAttributesID);
		this.topology = new BigObjectReference<GraphTopology>(topologyID);
	}

	@Override
	public void clearLocalData()
	{
	}

	public GraphTopology getTopology()
	{
		return topology.get();
	}

	protected A getVertexAttributes()
	{
		return vertexAttributes.get();
	}

	protected void setVertexAttributes(BigObjectReference<A> dhtReference)
	{
		this.vertexAttributes = dhtReference;
	}

	protected E getEdgeAttributes()
	{
		return edgeAttributes.get();
	}

	protected void setEdgeAttributes(BigObjectReference<E> dhtReference)
	{
		this.edgeAttributes = dhtReference;
	}

	public long getNumberOfVertices()
	{
		return getTopology().getOutAdjacencyTable().getNumberOfVertices();
	}

	public long getNumberOfEdges()
	{
		return getTopology().getOutAdjacencyTable().getNumberOfEdges();
	}

	static private int OperationCount = 0;

	static protected synchronized int incrAndGetOperationCount()
	{
		OperationCount++;
		return OperationCount;
	}

}
