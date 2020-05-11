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
import biggrph.dataset.EdgeListDataSet;
import biggrph.function.EdgeConsumer;
import biggrph.function.VertexConsumer;
import biggrph.function.VertexLongFunction;
import biggrph.function.VertexObjectFunction;
import biggrph.function.VertexPredicate;
import bigobject.BigObjectReference;
import bigobject.BigObjectRegistry;
import bigobject.DataSet;
import bigobject.LiveDistributedObject;
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
 * A graph with objects as vertex attributes and edge attributes. Such a graph
 * can be created from a {@link DataSet}, or a {@link BigAdjacencyTable} to set
 * the topology, together with an {@link ObjectDHT} to set the initial vertex
 * attributes.
 * 
 * A graph is a specific distributed object ({@link LiveDistributedObject}) ,
 * spread over a set of cluster nodes. As all distributed objects a graph has a
 * unique ID to identify it among all other distributed objects.
 * 
 * @author Nicolas Chleq
 *
 * @param <V>
 *            the type of the vertex attributes, must be serializable
 * @param <E>
 *            the type of the edge attributes, must be serializable
 */
public class BigGraph<V extends Serializable, E extends Serializable>
		extends AbstractBigGraph<ObjectDHT<V>, EdgeObjectAttributeTable<E>>
{

	protected BigGraph(String id, DHTAllocation allocation, String topologyId,
			String vAttributesID, String eAttributesID)
	{
		super(id, allocation, topologyId, vAttributesID, eAttributesID);
		setVertexAttributes(new BigObjectReference<ObjectDHT<V>>(vAttributesID));
		setEdgeAttributes(
				new BigObjectReference<EdgeObjectAttributeTable<E>>(eAttributesID));
	}

	/**
	 * Creates a graph from an instance of {@link GraphTopology} to initialize
	 * the topology of the graph and an instance of {@link ObjectDHT} to set the
	 * initial values of vertex attributes.
	 * 
	 * @param topology
	 * @param vertexAttributes
	 */
	public BigGraph(String newID, final GraphTopology topology,
			final ObjectDHT<V> vertexAttributes,
			final EdgeObjectAttributeTable<E> edgeAttributes)
	{
		this(newID, topology.getAllocation(), topology.getID(), vertexAttributes.getID(),
				edgeAttributes.getID());
	}

	/**
	 * Creates a graph from a {@link DataSet} to initialize the topology of the
	 * graph. Vertex and edge attributes have a default value of null for each
	 * vertex and edge. With this function, the ID of the distributed object
	 * representing the graph is derived from the ID of the dataset by appending
	 * the string "_graph".
	 * 
	 * @param dataset
	 *            An instance of {@link DataSet} or a subclass of DataSet.
	 *            Function {@link DataSet#load()} is used on this object to
	 *            build the topology object (usually a {@link BigAdjacencyTable}
	 *            instance). public BigGraph(EdgeListDataSet dataset) {
	 *            this(dataset.getID() + "_graph", dataset, false, false, 0); }
	 */

	/**
	 * Creates a graph from a {@link DataSet} to initialize the topology of the
	 * graph. The ID of the graph can be explicitly specified with this
	 * function.
	 * 
	 * @param id
	 *            The id of the distributed object representing the graph.
	 * @param dataset
	 *            An instance of {@link DataSet} or a subclass of DataSet.
	 *            public BigGraph(String id, EdgeListDataSet dataset, boolean
	 *            bidirectional, boolean multigraph, long vertexCountEstimate) {
	 *            this(id, new GraphTopology(id + TOPOLOGY_SUFFIX,
	 *            (DHTAllocation) dataset.getAllocation(), bidirectional,
	 *            multigraph, vertexCountEstimate).load(dataset)); }
	 */

	/**
	 * Creates a graph from an instance of {@link BigAdjacencyTable} to
	 * initialize the topology of the graph. The ID of the distributed object
	 * representing the graph is build by prepending the string "graph_" to the
	 * ID of the topology object.
	 * 
	 * @param topology
	 *            public BigGraph(final GraphTopology topology) { this("graph_"
	 *            + topology.getID(), topology); }
	 */

	/**
	 * Creates a graph with an explicitly defined ID from an instance of
	 * {@link BigAdjacencyTable} to initialize the topology of the graph.
	 * 
	 * @param id
	 * @param topology
	 *            public BigGraph(String id, final GraphTopology topology) {
	 *            this(id, topology.getAllocation(), topology.getID(), id +
	 *            VERTEX_ATTR_SUFFIX, id +
	 *            EDGE_ATTR_SUFFIX); @SuppressWarnings("unused") final
	 *            ObjectDHT<V> attributes = new ObjectDHT<V>(id +
	 *            VERTEX_ATTR_SUFFIX,
	 *            topology.getAllocation()); @SuppressWarnings("unused") final
	 *            EdgeObjectAttributeTable<E> eAttributes = new
	 *            EdgeObjectAttributeTable<E>( id + EDGE_ATTR_SUFFIX,
	 *            topology.getAllocation()); }
	 */

	/**
	 * Creates a graph from an instance of {@link BigAdjacencyTable} to
	 * initialize the topology of the graph and an instance of {@link ObjectDHT}
	 * to set the initial values of vertex attributes. The ID of the graph can
	 * be explicitly defined with this function.
	 * 
	 * @param id
	 * @param topology
	 * @param vertexAttributes
	 *            public BigGraph(String id, final GraphTopology topology, final
	 *            ObjectDHT<V> vertexAttributes) { this(id,
	 *            topology.getAllocation(), topology.getID(),
	 *            vertexAttributes.getID(), id + EDGE_ATTR_SUFFIX); }
	 */

	/*
	 * public BigGraph(String id, final GraphTopology topology, final
	 * ObjectDHT<V> vertexAttributes, final EdgeObjectAttributeTable<E>
	 * edgeAttributes) { this(id, topology.getAllocation(), topology.getID(),
	 * vertexAttributes.getID(), edgeAttributes.getID()); }
	 */

	public V getVertexAttribute(long vertexId)
	{
		return getVertexAttributes().get(vertexId);
	}

	public void setVertexAttribute(long vertexId, V value)
	{
		getVertexAttributes().set(vertexId, value);
	}

	public E getEdgeAttribute(long src, long dst)
	{
		return getEdgeAttributes().getEdgeAttribute(src, dst);
	}

	public void setEdgeAttribute(long src, long dst, E value)
	{
		getEdgeAttributes().setEdgeAttribute(src, dst, value);
	}

	public boolean edgeHasAttribute(long src, long dst)
	{
		return getEdgeAttributes().containsEdge(src, dst);
	}

	public void forEachVertex(final VertexConsumer<V> function)
	{
		new OneNodeOneRequest<Long>()
		{
			@Override
			protected ComputationRequest<Long> createComputationRequestForNode(
					OctojusNode node)
			{
				@SuppressWarnings("synthetic-access")
				ForEachGraphVertexJob<V, E> job = new ForEachGraphVertexJob<V, E>();
				job.graphID = BigGraph.this.getID();
				job.function = function;
				return job;
			}
		}.execute(getAllocation().getNodes());
	}

	@SuppressWarnings("serial")
	static private class ForEachGraphVertexJob<V extends Serializable, E extends Serializable>
			extends ComputationRequest<Long>
	{
		protected String graphID;
		protected VertexConsumer<V> function;

		@SuppressWarnings("boxing")
		@Override
		protected Long compute() throws Throwable
		{
			@SuppressWarnings("unchecked")
			BigGraph<V, E> graph = (BigGraph<V, E>) BigObjectRegistry.defaultRegistry
					.get(graphID);
			BigAdjacencyTable outTopology = graph.getTopology().getOutAdjacencyTable();
			Iterator<LongObjectCursor<LongSet>> iterator = outTopology.getLocalData()
					.iterator();
			long vertexCount = 0;
			while (iterator.hasNext())
			{
				LongObjectCursor<LongSet> element = iterator.next();
				long vertexId = element.key;
				V attribute = graph.getVertexAttribute(vertexId);
				function.accept(graph, vertexId, attribute);
				vertexCount++;
			}
			System.out.println(
					"BigGraph.forEachVertex: processed " + vertexCount + " vertices");
			return vertexCount;
		}
	}

	public void forEachEdge(final EdgeConsumer<V, E> function)
	{
		new OneNodeOneRequest<NoReturn>()
		{
			@Override
			protected ComputationRequest<NoReturn> createComputationRequestForNode(
					OctojusNode n)
			{
				@SuppressWarnings("synthetic-access")
				ForEachEdgeJob<V, E> job = new ForEachEdgeJob<V, E>();
				job.graphID = getID();
				job.function = function;
				return job;
			}

		}.execute(getAllocation().getNodes());
	}

	@SuppressWarnings("serial")
	static private class ForEachEdgeJob<V extends Serializable, E extends Serializable>
			extends ComputationRequest<NoReturn>
	{
		protected String graphID;
		protected EdgeConsumer<V, E> function;

		@Override
		protected NoReturn compute() throws Throwable
		{
			@SuppressWarnings("unchecked")
			BigGraph<V, E> graph = (BigGraph<V, E>) BigObjectRegistry.defaultRegistry
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
							graph.getEdgeAttribute(srcVertexId, dstVertexId));
					edgeCount++;
				}
			}
			System.out.println("BigGraph.forEachEdge: processed " + edgeCount + " edges");
			return null;
		}
	}

	public <R extends Serializable> ObjectDHT<R> mapVertices(
			final VertexObjectFunction<V, R> function)
	{
		final String resultID = getID() + "_mapVertices_" + incrAndGetOperationCount();
		ObjectDHT<R> results = new ObjectDHT<R>(resultID,
				(DHTAllocation) getAllocation());
		new OneNodeOneRequest<NoReturn>()
		{
			@Override
			protected ComputationRequest<NoReturn> createComputationRequestForNode(
					OctojusNode n)
			{
				MapVertices2ObjectJob<V, R> job = new MapVertices2ObjectJob<V, R>();
				job.function = function;
				job.graphID = getID();
				job.resultID = resultID;
				return null;
			}

		}.execute(getAllocation().getNodes());
		return results;
	}

	@SuppressWarnings("serial")
	static private class MapVertices2ObjectJob<V extends Serializable, R extends Serializable>
			extends ComputationRequest<NoReturn>
	{
		protected String graphID;
		protected String resultID;
		VertexObjectFunction<V, R> function;

		@Override
		protected NoReturn compute() throws Throwable
		{
			@SuppressWarnings("unchecked")
			BigGraph<V, ?> graph = (BigGraph<V, ?>) BigObjectRegistry.defaultRegistry
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
				V attribute = graph.getVertexAttribute(vertexId);
				R result = function.apply(graph, vertexId, attribute);
				results.set(vertexId, result);
				vertexCount++;
			}
			System.out.println(
					"BigGraph.mapVertices2Long: processed " + vertexCount + " vertices");
			return null;
		}
	}

	public LongDHT mapVertices2Long(final VertexLongFunction<V> function)
	{
		final String resultID = getID() + "_mapVertices2L_" + incrAndGetOperationCount();
		LongDHT results = new LongDHT(resultID, (DHTAllocation) getAllocation());
		new OneNodeOneRequest<NoReturn>()
		{
			@Override
			protected ComputationRequest<NoReturn> createComputationRequestForNode(
					OctojusNode n)
			{
				MapVertices2LongJob<V> job = new MapVertices2LongJob<V>();
				job.function = function;
				job.graphID = getID();
				job.resultID = resultID;
				return job;
			}

		}.execute(getAllocation().getNodes());
		return results;
	}

	@SuppressWarnings("serial")
	static private class MapVertices2LongJob<V extends Serializable>
			extends ComputationRequest<NoReturn>
	{
		protected String graphID;
		protected String resultID;
		protected VertexLongFunction<V> function;

		@Override
		protected NoReturn compute() throws Throwable
		{
			@SuppressWarnings("unchecked")
			BigGraph<V, ?> graph = (BigGraph<V, ?>) BigObjectRegistry.defaultRegistry
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
				V attribute = graph.getVertexAttribute(vertexId);
				long result = function.apply(graph, vertexId, attribute);
				results.set(vertexId, result);
				vertexCount++;
			}
			System.out.println(
					"BigGraph.mapVertices2Long: processed " + vertexCount + " vertices");
			return null;
		}
	}

	/**
	 * Returns a new graph built with the vertices of the original graph for
	 * which the predicate evaluation returns true, and the edges that connect
	 * two such vertices.
	 * 
	 * @param predicate
	 * @return
	 */
	public BigGraph<V, E> filter(VertexPredicate<V> predicate)
	{
		String newID = getID() + "_filter_" + incrAndGetOperationCount();
		DHTAllocation allocation = (DHTAllocation) getAllocation();
		GraphTopology newTopology = new GraphTopology(newID + TOPOLOGY_SUFFIX, allocation,
				getTopology().isBiDirectional(), getTopology().isMultigraph(), 0);
		ObjectDHT<V> newVertexAttributes = new ObjectDHT<V>(newID + VERTEX_ATTR_SUFFIX,
				allocation);
		EdgeObjectAttributeTable<E> newEdgeAttributes = new EdgeObjectAttributeTable<E>(
				newID + EDGE_ATTR_SUFFIX, allocation);

		new OneNodeOneRequest<NoReturn>()
		{
			@Override
			protected ComputationRequest<NoReturn> createComputationRequestForNode(
					OctojusNode n)
			{
				GraphFilterJob<V, E> job = new GraphFilterJob<V, E>();
				job.graphID = getID();
				job.newGraphID = newID;
				job.predicate = predicate;
				return job;
			}
		}.execute(allocation.getNodes());

		BigGraph<V, E> newGraph = new BigGraph<V, E>(newID, newTopology,
				newVertexAttributes, newEdgeAttributes);
		return newGraph;
	}

	@SuppressWarnings("serial")
	static private class GraphFilterJob<V extends Serializable, E extends Serializable>
			extends ComputationRequest<NoReturn>
	{
		String graphID;
		String newGraphID;
		VertexPredicate<V> predicate;

		/*
		 * Computation is done in 3 phases: - The first phase iterates over all
		 * local vertices, evaluates the predicate on these vertices and puts
		 * the one that are kept in the topology of the new graph. While
		 * iterating over the kept vertices, all the destination vertices of the
		 * edges are handled in the following way : if the vertex is local, the
		 * predicate is evaluated, if true the edge is added to the new
		 * topology. If the vertex is not local it is stored in a dedicated
		 * structure based on the cluster node to which it pertains. - The
		 * second phase performs the remote evaluation of the predicate for all
		 * other cluster nodes. Results are sent back to the requesting node. -
		 * Third phase use these results to complete the building of the new
		 * topology
		 */
		@Override
		protected NoReturn compute() throws Throwable
		{
			@SuppressWarnings("unchecked")
			BigGraph<V, E> graph = (BigGraph<V, E>) BigObjectRegistry.defaultRegistry
					.get(graphID);
			DHTAllocation allocation = (DHTAllocation) graph.getAllocation();
			GraphTopology newTopology = (GraphTopology) BigObjectRegistry.defaultRegistry
					.get(newGraphID + TOPOLOGY_SUFFIX);
			@SuppressWarnings("unchecked")
			ObjectDHT<V> newVertexAttributes = (ObjectDHT<V>) BigObjectRegistry.defaultRegistry
					.get(newGraphID + VERTEX_ATTR_SUFFIX);
			@SuppressWarnings("unchecked")
			EdgeObjectAttributeTable<E> newEdgeAttributes = (EdgeObjectAttributeTable<E>) BigObjectRegistry.defaultRegistry
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
				V srcAttr = graph.getVertexAttribute(srcVertex);
				if (predicate.test(graph, srcVertex, srcAttr))
				{
					newTopology.getOutAdjacencyTable().__local_ensureExists(srcVertex);
					newVertexAttributes.set(srcVertex, srcAttr);
					for (LongCursor cursor : pair.value)
					{
						long dstVertex = cursor.value;
						if (allocation.isLocalElement(dstVertex))
						{
							V dstAttr = graph.getVertexAttribute(dstVertex);
							if (predicate.test(graph, dstVertex, dstAttr))
							{
								newTopology.getOutAdjacencyTable()
										.__local_ensureExists(dstVertex);
								newTopology.getOutAdjacencyTable().__local_add(srcVertex,
										dstVertex);
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
					TCPConnection connection = graph
							.connectTo(graph.remoteVertexPredicateEval, node);
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
						{
							synchronized (trueVertices)
							{
								trueVertices.add(vertex);
							}
						}
					}
				}
			};
			// Alternate sequential implementation.
			// for (OctojusNode node : remoteVertices.keySet())
			// {
			// TCPConnection connection =
			// graph.connectTo(graph.remoteVertexPredicateEval, node);
			// connection.out.writeObject(predicate);
			// int nVertices = remoteVertices.get(node).size();
			// connection.out.writeLong(nVertices);
			// for (LongCursor v : remoteVertices.get(node))
			// {
			// connection.out.writeLong(v.value);
			// }
			// for (int i = 0; i < nVertices; i++)
			// {
			// long vertex = connection.in.readLong();
			// byte res = connection.in.readByte();
			// if (res != 0)
			// trueVertices.add(vertex);
			// }
			// }

			// Third phase: complete the building of the new topology. Use the
			// already selected source
			// vertices to limit iteration.
			Iterator<LongObjectCursor<LongSet>> iterator2 = newTopology
					.getOutAdjacencyTable().getLocalData().iterator();
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
						newTopology.getOutAdjacencyTable().__local_add(srcVertex,
								dstVertex);
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
		@SuppressWarnings("unchecked")
		@Override
		public void serveThrough(FullDuplexDataConnection2 connection)
		{
			VertexPredicate<V> predicate;
			long size;
			try
			{
				predicate = (VertexPredicate<V>) connection.in.readObject();
				size = connection.in.readLong();
				LongArrayList vertices = new LongArrayList((int) size);
				// System.out.println("Receiving " + size + " vertices");
				for (int i = 0; i < size; i++)
				{
					long vertex = connection.in.readLong();
					vertices.add(vertex);
				}
				// System.out.println("Checking predicate on " + size + "
				// vertices");
				LongByteOpenHashMap results = new LongByteOpenHashMap((int) size);
				for (LongCursor c : LongCursor.fromHPPC(vertices))
				{
					long vertex = c.value;
					V attr = getVertexAttribute(vertex);
					boolean res = predicate.test(BigGraph.this, vertex, attr);
					results.put(vertex, (byte) (res ? 1 : 0));
				}
				// System.out.println("Sending results for " + size + "
				// vertices");
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
