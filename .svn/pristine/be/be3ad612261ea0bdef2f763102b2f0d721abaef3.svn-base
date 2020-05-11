package biggrph;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import octojus.ComputationRequest;
import octojus.NoReturn;
import octojus.OctojusNode;
import octojus.OneNodeOneRequest;
import octojus.OneNodeOneThread;
import toools.collection.bigstuff.longset.LongCursor;
import toools.io.FullDuplexDataConnection2;
import toools.net.TCPConnection;
import biggrph.function.EdgeLongConsumer;
import biggrph.function.VertexLongConsumer;
import biggrph.function.VertexLongLongFunction;
import biggrph.function.VertexLongObjectFunction;
import biggrph.function.VertexLongPredicate;
import bigobject.Allocation;
import bigobject.BigObjectReference;
import bigobject.BigObjectRegistry;
import bigobject.Service;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongByteOpenHashMap;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.cursors.LongByteCursor;
import com.carrotsearch.hppc.cursors.LongObjectCursor;

import dht.DHTAllocation;
import dht.LongDHT;
import dht.ObjectDHT;
import dht.set.LongSet;

/**
 * A graph with long integers as vertex attributes and edge attributes.
 * 
 * @author Nicolas Chleq
 *
 */
public class BigGraphLong extends AbstractBigGraph<LongDHT, EdgeLongAttributeTable>
{

	protected BigGraphLong(String id, Allocation allocation, String topologyID,
			String vAttributesID, String eAttributesID)
	{
		super(id, allocation, topologyID, vAttributesID, eAttributesID);
		setVertexAttributes(new BigObjectReference<LongDHT>(vAttributesID));
		setEdgeAttributes(new BigObjectReference<EdgeLongAttributeTable>(eAttributesID));
	}

	/*
	 * public BigGraphLong(EdgeListDataSet dataset)
	 * {
	 * this(dataset.getID() + "_graph", dataset);
	 * }
	 * 
	 * public BigGraphLong(String id, EdgeListDataSet dataset)
	 * {
	 * this(id, dataset.load());
	 * }
	 */
	public BigGraphLong(final GraphTopology topology)
	{
		this("graph_" + topology.getID(), topology);
	}

	public BigGraphLong(String id, final GraphTopology topology)
	{
		this(id, topology.getAllocation(), topology.getID(), id + VERTEX_ATTR_SUFFIX, id
				+ EDGE_ATTR_SUFFIX);
		@SuppressWarnings("unused")
		final LongDHT vAttributes = new LongDHT(id + VERTEX_ATTR_SUFFIX,
				topology.getAllocation());
		@SuppressWarnings("unused")
		final EdgeLongAttributeTable eAttributes = new EdgeLongAttributeTable(id
				+ EDGE_ATTR_SUFFIX, topology.getAllocation(), 0);
	}

	public BigGraphLong(final GraphTopology topology, final LongDHT vertexAttributes)
	{
		this("graph_" + topology.getID() + vertexAttributes.getID(), topology,
				vertexAttributes);
	}

	public BigGraphLong(String id, final GraphTopology topology,
			final LongDHT vertexAttributes)
	{
		this(id, topology.getAllocation(), topology.getID(), vertexAttributes.getID(), id
				+ EDGE_ATTR_SUFFIX);
	}

	public BigGraphLong(String id, final GraphTopology topology,
			final LongDHT vertexAttributes, final EdgeLongAttributeTable edgeAttributes)
	{
		this(id, topology.getAllocation(), topology.getID(), vertexAttributes.getID(),
				edgeAttributes.getID());
	}

	public long getVertexAttribute(long vertexId)
	{
		return getVertexAttributes().get(vertexId);
	}

	public void setVertexAttribute(long vertexId, long value)
	{
		getVertexAttributes().set(vertexId, value);
	}

	public long getEdgeAttribute(long src, long dst)
	{
		return getEdgeAttributes().getEdgeAttribute(src, dst);
	}

	public void setEdgeAttribute(long src, long dst, long value)
	{
		getEdgeAttributes().setEdgeAttribute(src, dst, value);
	}

	public boolean edgeHasAttribute(long src, long dst)
	{
		return getEdgeAttributes().containsEdge(src, dst);
	}

	public void forEachVertex(final VertexLongConsumer function)
	{
		new OneNodeOneRequest<Long>()
		{
			@Override
			protected ComputationRequest<Long> createComputationRequestForNode(
					OctojusNode node)
			{
				ForEachGraphVertexJob job = new ForEachGraphVertexJob();
				job.graphID = BigGraphLong.this.getID();
				job.function = function;
				return job;
			}
		}.execute(getAllocation().getNodes());
	}

	@SuppressWarnings("serial")
	static private class ForEachGraphVertexJob extends ComputationRequest<Long>
	{
		protected String graphID;
		protected VertexLongConsumer function;

		@SuppressWarnings("boxing")
		@Override
		protected Long compute() throws Throwable
		{
			BigGraphLong graph = (BigGraphLong) BigObjectRegistry.defaultRegistry
					.get(graphID);
			BigAdjacencyTable outTopology = graph.getTopology().getOutAdjacencyTable();
			Iterator<LongObjectCursor<LongSet>> iterator = outTopology.getLocalData()
					.iterator();
			long vertexCount = 0;
			while (iterator.hasNext())
			{
				LongObjectCursor<LongSet> element = iterator.next();
				long vertexId = element.key;
				long attribute = graph.getVertexAttribute(vertexId);
				function.accept(graph, vertexId, attribute);
				vertexCount++;
			}
			System.out.println("BigGraphLong.forEachVertex: processed " + vertexCount
					+ " vertices");
			return vertexCount;
		}
	}

	public void forEachEdge(final EdgeLongConsumer function)
	{
		new OneNodeOneRequest<NoReturn>()
		{
			@Override
			protected ComputationRequest<NoReturn> createComputationRequestForNode(
					OctojusNode n)
			{
				ForEachEdgeJob job = new ForEachEdgeJob();
				job.graphID = getID();
				job.function = function;
				return job;
			}

		}.execute(getAllocation().getNodes());
	}

	@SuppressWarnings("serial")
	static private class ForEachEdgeJob extends ComputationRequest<NoReturn>
	{
		protected String graphID;
		protected EdgeLongConsumer function;

		@Override
		protected NoReturn compute() throws Throwable
		{
			BigGraphLong graph = (BigGraphLong) BigObjectRegistry.defaultRegistry
					.get(graphID);
			BigAdjacencyTable outTopology = graph.getTopology().getOutAdjacencyTable();
			Iterator<LongObjectCursor<LongSet>> srcIterator = outTopology.getLocalData()
					.iterator();
			long edgeCount = 0;
			while (srcIterator.hasNext())
			{
				LongObjectCursor<LongSet> srcElement = srcIterator.next();
				long srcVertexId = srcElement.key;
				Iterator<LongCursor> dstIterator = outTopology.getLocalData()
						.get(srcVertexId).iterator();
				while (dstIterator.hasNext())
				{
					LongCursor dstElement = dstIterator.next();
					long dstVertexId = dstElement.value;
					function.accept(graph, srcVertexId, dstVertexId,
							graph.getVertexAttribute(srcVertexId),
							graph.getVertexAttribute(dstVertexId));
					edgeCount++;
				}
			}
			System.out.println("BigGraphLong.forEachEdge: processed " + edgeCount
					+ " edges");
			return null;
		}
	}

	public <R extends Serializable> ObjectDHT<R> mapVertices(
			final VertexLongObjectFunction<R> function)
	{
		final String resultID = getID() + "_mapVertices_" + incrAndGetOperationCount();
		ObjectDHT<R> results = new ObjectDHT<R>(resultID, (DHTAllocation) getAllocation());
		new OneNodeOneRequest<NoReturn>()
		{
			@Override
			protected ComputationRequest<NoReturn> createComputationRequestForNode(
					OctojusNode n)
			{
				MapVertices2ObjectJob<R> job = new MapVertices2ObjectJob<R>();
				job.function = function;
				job.graphID = getID();
				job.resultID = resultID;
				return null;
			}

		}.execute(getAllocation().getNodes());
		return results;
	}

	@SuppressWarnings("serial")
	static private class MapVertices2ObjectJob<R extends Serializable> extends
			ComputationRequest<NoReturn>
	{
		protected String graphID;
		protected String resultID;
		VertexLongObjectFunction<R> function;

		@Override
		protected NoReturn compute() throws Throwable
		{
			BigGraphLong graph = (BigGraphLong) BigObjectRegistry.defaultRegistry
					.get(graphID);
			BigAdjacencyTable outTopology = graph.getTopology().getOutAdjacencyTable();
			Iterator<LongObjectCursor<LongSet>> iterator = outTopology.getLocalData()
					.iterator();
			@SuppressWarnings("unchecked")
			ObjectDHT<R> results = (ObjectDHT<R>) BigObjectRegistry.defaultRegistry
					.get(resultID);
			long vertexCount = 0;
			while (iterator.hasNext())
			{
				LongObjectCursor<LongSet> element = iterator.next();
				long vertexId = element.key;
				long attribute = graph.getVertexAttribute(vertexId);
				R result = function.apply(graph, vertexId, attribute);
				results.set(vertexId, result);
				vertexCount++;
			}
			System.out.println("BigGraphLong.mapVertices: processed " + vertexCount
					+ " vertices");
			return null;
		}
	}

	public LongDHT mapVertices2Long(final VertexLongLongFunction function)
	{
		final String resultID = getID() + "_mapVertices2L_" + incrAndGetOperationCount();
		LongDHT results = new LongDHT(resultID, (DHTAllocation) getAllocation());
		new OneNodeOneRequest<NoReturn>()
		{
			@Override
			protected ComputationRequest<NoReturn> createComputationRequestForNode(
					OctojusNode n)
			{
				MapVertices2LongJob job = new MapVertices2LongJob();
				job.function = function;
				job.graphID = getID();
				job.resultID = resultID;
				return job;
			}

		}.execute(getAllocation().getNodes());
		return results;
	}

	@SuppressWarnings("serial")
	static private class MapVertices2LongJob extends ComputationRequest<NoReturn>
	{
		protected String graphID;
		protected String resultID;
		protected VertexLongLongFunction function;

		@Override
		protected NoReturn compute() throws Throwable
		{
			BigGraphLong graph = (BigGraphLong) BigObjectRegistry.defaultRegistry
					.get(graphID);
			BigAdjacencyTable outTopology = graph.getTopology().getOutAdjacencyTable();
			Iterator<LongObjectCursor<LongSet>> iterator = outTopology.getLocalData()
					.iterator();
			LongDHT results = (LongDHT) BigObjectRegistry.defaultRegistry.get(resultID);
			long vertexCount = 0;
			while (iterator.hasNext())
			{
				LongObjectCursor<LongSet> element = iterator.next();
				long vertexId = element.key;
				long attribute = graph.getVertexAttribute(vertexId);
				long result = function.apply(graph, vertexId, attribute);
				results.set(vertexId, result);
				vertexCount++;
			}
			System.out.println("BigGraph.mapVertices2Long: processed " + vertexCount
					+ " vertices");
			return null;
		}
	}

	// public BigGraphLong filter(VertexLongPredicate predicate)
	// {
	// String newID = getID() + "_filter_" + incrAndGetOperationCount();
	// DHTAllocation allocation = (DHTAllocation) getAllocation();
	// BigAdjacencyTable newTopology = new BigAdjacencyTable(newID +
	// TOPOLOGY_SUFFIX,
	// allocation);
	// LongDHT newVertexAttributes = new LongDHT(newID + VERTEX_ATTR_SUFFIX,
	// allocation);
	// EdgeLongAttributeTable newEdgeAttributes = new
	// EdgeLongAttributeTable(newID
	// + EDGE_ATTR_SUFFIX, allocation, 0);
	//
	// new OneNodeOneRequest<NoReturn>()
	// {
	// @Override
	// protected ComputationRequest<NoReturn> createComputationRequestForNode(
	// OctojusNode n)
	// {
	// GraphLongFilterJob job = new GraphLongFilterJob();
	// job.graphID = getID();
	// job.newGraphID = newID;
	// job.predicate = predicate;
	// return job;
	// }
	// }.execute(allocation.getNodes());
	//
	// BigGraphLong newGraph = new BigGraphLong(newID, newTopology,
	// newVertexAttributes,
	// newEdgeAttributes);
	// return newGraph;
	// }

	@SuppressWarnings({ "serial", "unused" })
	static private class GraphLongFilterJob extends ComputationRequest<NoReturn>
	{
		String graphID;
		String newGraphID;
		VertexLongPredicate predicate;

		@Override
		protected NoReturn compute() throws Throwable
		{
			BigGraphLong graph = (BigGraphLong) BigObjectRegistry.defaultRegistry
					.get(graphID);
			DHTAllocation allocation = (DHTAllocation) graph.getAllocation();
			BigAdjacencyTable newTopology = (BigAdjacencyTable) BigObjectRegistry.defaultRegistry
					.get(newGraphID + TOPOLOGY_SUFFIX);
			LongDHT newVertexAttributes = (LongDHT) BigObjectRegistry.defaultRegistry
					.get(newGraphID + VERTEX_ATTR_SUFFIX);
			EdgeLongAttributeTable newEdgeAttributes = (EdgeLongAttributeTable) BigObjectRegistry.defaultRegistry
					.get(newGraphID + EDGE_ATTR_SUFFIX);

			// Where remote vertices are stored when they are discovered
			Map<OctojusNode, LongArrayList> remoteVertices = new HashMap<OctojusNode, LongArrayList>();
			int remoteVertexCount = 0;
			Iterator<LongObjectCursor<LongSet>> iterator = graph.getTopology()
					.getOutAdjacencyTable().getLocalData().iterator();
			while (iterator.hasNext())
			{
				LongObjectCursor<LongSet> pair = iterator.next();
				long srcVertex = pair.key;
				long srcAttr = graph.getVertexAttribute(srcVertex);
				if (predicate.test(graph, srcVertex, srcAttr))
				{
					newTopology.__local_ensureExists(srcVertex);
					newVertexAttributes.set(srcVertex, srcAttr);
					for (LongCursor cursor : pair.value)
					{
						long dstVertex = cursor.value;
						if (allocation.isLocalElement(dstVertex))
						{
							long dstAttr = graph.getVertexAttribute(dstVertex);
							if (predicate.test(graph, dstVertex, dstAttr))
							{
								newTopology.__local_ensureExists(dstVertex);
								newTopology.__local_add(srcVertex, dstVertex);
								newVertexAttributes.set(dstVertex, dstAttr);
								if (graph.getEdgeAttributes().containsEdge(srcVertex,
										dstVertex))
									newEdgeAttributes.setEdgeAttribute(srcVertex,
											dstVertex,
											graph.getEdgeAttribute(srcVertex, dstVertex));
							}
						}
						else
						{
							OctojusNode node = allocation.getOwnerNode(dstVertex);
							if (remoteVertices.get(node) == null)
								remoteVertices.put(node, new LongArrayList());
							remoteVertices.get(node).add(dstVertex);
							remoteVertexCount++;
						}
					}
				}
			}
			// Second phase: remote evaluation of the predicate
			LongOpenHashSet trueVertices = new LongOpenHashSet(remoteVertexCount);
			new OneNodeOneThread(remoteVertices.keySet())
			{
				@Override
				protected void process(OctojusNode node) throws Throwable
				{
					TCPConnection connection = graph.connectTo(
							graph.remoteVertexPredicateEval, node);
					connection.out.writeObject(predicate);
					int nVertices = remoteVertices.get(node).size();
					connection.out.writeLong(nVertices);
					for (LongCursor v : LongCursor.fromHPPC(remoteVertices.get(node)))
					{
						connection.out.writeLong(v.value);
					}
					for (int i = 0; i < nVertices; i++)
					{
						long vertex = connection.in.readLong();
						byte res = connection.in.readByte();
						if (res != 0)
							synchronized (trueVertices)
							{
								trueVertices.add(vertex);
							}
					}
				}
			};
			// Third phase: complete the building of the new topology.
			Iterator<LongObjectCursor<LongSet>> iterator2 = newTopology.getLocalData()
					.iterator();
			while (iterator2.hasNext())
			{
				LongObjectCursor<LongSet> pair = iterator2.next();
				long srcVertex = pair.key;
				LongSet destinations = graph.getTopology().getOutAdjacencyTable()
						.getLocalData().get(srcVertex);
				for (LongCursor c : destinations)
				{
					long dstVertex = c.value;
					if (( ! allocation.isLocalElement(dstVertex))
							&& trueVertices.contains(dstVertex))
					{
						newTopology.__local_add(srcVertex, dstVertex);
						if (graph.getEdgeAttributes().containsEdge(srcVertex, dstVertex))
							newEdgeAttributes.setEdgeAttribute(srcVertex, dstVertex,
									graph.getEdgeAttribute(srcVertex, dstVertex));
					}
				}
			}

			return null;
		}
	}

	private Service remoteVertexPredicateEval = new Service()
	{
		@Override
		public void serveThrough(FullDuplexDataConnection2 connection)
		{
			VertexLongPredicate predicate;
			long size;
			try
			{
				predicate = (VertexLongPredicate) connection.in.readObject();
				size = connection.in.readLong();
				LongArrayList vertices = new LongArrayList((int) size);
				// System.out.println("Receiving " + size + " vertices");
				for (int i = 0; i < size; i++)
				{
					long vertex = connection.in.readLong();
					vertices.add(vertex);
				}
				// System.out.println("Checking predicate on " + size +
				// " vertices");
				LongByteOpenHashMap results = new LongByteOpenHashMap((int) size);
				for (LongCursor c : LongCursor.fromHPPC(vertices))
				{
					long vertex = c.value;
					long attr = getVertexAttribute(vertex);
					boolean res = predicate.test(BigGraphLong.this, vertex, attr);
					results.put(vertex, (byte) (res ? 1 : 0));
				}
				// System.out.println("Sending results for " + size +
				// " vertices");
				for (Iterator<LongByteCursor> iterator = results.iterator(); iterator
						.hasNext();)
				{
					LongByteCursor c = iterator.next();
					connection.out.writeLong(c.key);
					connection.out.writeByte(c.value);
				}
				// System.out.println("Sending done.");
			}
			catch (ClassNotFoundException | IOException e)
			{
				e.printStackTrace();
				return;
			}
		}
	};

}
