package biggrph;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import ldjo.bsp.BSPable;
import octojus.ComputationRequest;
import octojus.NoReturn;
import octojus.OctojusNode;
import octojus.OneNodeOneRequest;
import toools.io.file.RegularFile;
import toools.math.DistributionForLongs;
import toools.text.TextUtilities;
import toools.util.LongPredicate;
import biggrph._algo.count.EdgeAndVertexCount;
import biggrph._algo.count.NumberOfEdgesAlgorithm;
import biggrph._algo.count.NumberOfVerticesAlgorithm;
import biggrph.algo.connected_components.AssignConnectedComponents;
import biggrph.algo.connected_components.ConnectedComponentsSize;
import biggrph.algo.connected_components.EnumeratingConnectedComponents;
import biggrph.algo.degree.MaxDegreeAlgorithmResult;
import biggrph.algo.diameter.DiameterResult;
import biggrph.algo.diameter.ifub.IFub;
import biggrph.algo.diameter.ifub.IfubListener;
import biggrph.algo.diameter.ifub.IfubStdoutLogger;
import biggrph.algo.randomVertex.PickRandomVertices2;
import biggrph.algo.search.bfs.BFS.BFSResult;
import biggrph.dataset.BigGrphDataSet;
import bigobject.BigObjectReference;
import bigobject.BigObjectRegistry;
import bigobject.DataSet;

import com.carrotsearch.hppc.LongObjectOpenHashMap;

import dht.DHT;
import dht.DHTAllocation;
import dht.set.HashLongSet64;
import dht.set.LongSet;

public class GraphTopology extends DHT<Serializable, Serializable> implements BSPable
{
	private BigObjectReference<BigAdjacencyTable> outEdges;
	protected final static String OUT_EDGES_SUFFIX = "/out";

	private BigObjectReference<BigAdjacencyTable> inEdges;
	protected final static String IN_EDGES_SUFFIX = "/in";

	private boolean biDirectional = false;
	private boolean multigraph = false;
	private boolean undirected = false;

	/**
	 * Creates a graph as a big distributed adjacency table.
	 *  
	 * @param dataset The dataset that will be loaded to build the structure of the graph
	 * @param biDirectional if true, all reverse edges found in the dataset are created in the graph 
	 * @param multigraph if true allows several edges between two vertices
	 * @param vertexCountEstimate an upper bound estimate of the number of vertices in the graph. It is
	 *        used to allocate internal structures with sufficient capacity to avoid resizing them
	 *        during creation of the graph.  
	 */
	public GraphTopology(BigGrphDataSet dataset, boolean biDirectional,
			boolean multigraph, long vertexCountEstimate)
	{
		this(dataset.getName(), dataset.getAllocation(), biDirectional, multigraph,
				false, vertexCountEstimate);
		load(dataset);
//		System.out.println("Saving to local disk");
//		save();
	}
	
	/**
	 * 
	 * @param dataset The dataset that will be loaded to build the structure of the graph
	 * @param biDirectional if true, all reverse edges found in the dataset are created in the graph 
	 * @param multigraph if true allows several edges between two vertices
	 * @param undirected
	 * @param vertexCountEstimate an upper bound estimate of the number of vertices in the graph. It is
	 *        used to allocate internal structures with sufficient capacity to avoid resizing them
	 *        during creation of the graph.  
	 */
	public GraphTopology(BigGrphDataSet dataset, boolean biDirectional,
			boolean multigraph, boolean undirected, long vertexCountEstimate)
	{
		this(dataset.getName(), dataset.getAllocation(), biDirectional, multigraph,
				undirected, vertexCountEstimate);
		load(dataset);
//		System.out.println("Saving to local disk");
//		save();
	}

	static public GraphTopology createBidirectionalGraph(BigGrphDataSet dataset, long vertexCountEstimate)
	{
		return new GraphTopology(dataset, true, false, false, vertexCountEstimate);
	}
	
	static public GraphTopology createUndirectedGraph(BigGrphDataSet dataset, long vertexCountEstimate)
	{
		return new GraphTopology(dataset, false, false, true, vertexCountEstimate);
	}

	public GraphTopology(String id, DHTAllocation allocation, boolean biDirectional,
			boolean multigraph, long vertexCountEstimate)
	{
		this(id, allocation, biDirectional, multigraph, false, vertexCountEstimate);
	}
	
	public GraphTopology(String id, DHTAllocation allocation, boolean biDirectional,
			boolean multigraph, boolean undirected, long vertexCountEstimate)
	{
		super(id, allocation, biDirectional, multigraph, undirected, vertexCountEstimate);
		this.outEdges = new BigObjectReference<BigAdjacencyTable>(id + OUT_EDGES_SUFFIX);
		this.biDirectional = biDirectional;
		this.multigraph = multigraph;
		this.undirected = undirected;

		if (biDirectional)
		{
			this.inEdges = new BigObjectReference<BigAdjacencyTable>(
					id + IN_EDGES_SUFFIX);
		}
		// Actually create the BigAdjacencyTable instances only on the master
		// cluster node this will trigger the deployment of the adjacency tables

		
		if (allocation.getInitiatorNode().isLocalNode())
		{
			new BigAdjacencyTable(id + OUT_EDGES_SUFFIX, allocation, vertexCountEstimate,
					multigraph);
			
			if (biDirectional)
			{
				new BigAdjacencyTable(id + IN_EDGES_SUFFIX, allocation,
						vertexCountEstimate, multigraph);
			}
		}
	}

	@Override
	public String toString()
	{
		return toString(true);
	}

	public String toString(boolean humanNumbers)
	{
		String s = getID() + ": ";
		s += humanNumbers ? TextUtilities.toHumanString(getNumberOfVertices())
				: getNumberOfVertices();
		s += " vertices, ";
		s += humanNumbers ? TextUtilities.toHumanString(getNumberOfEdges())
				: getNumberOfEdges();
		s += " edges";
		return s;
	}

	public long pickRandomVertex(Random random)
	{
		return pickRandomVertices(random, 1).iterator().next().value;
	}

	public LongSet pickRandomVertices(Random random, int n)
	{
		HashLongSet64 r = new HashLongSet64();
		PickRandomVertices2 rw = new PickRandomVertices2(this, n, new Random());
		r.addAll(rw.execute().getVertices());
		rw.delete();
		return r;
	}

	public BigAdjacencyTable getOutAdjacencyTable()
	{
		return outEdges.get();
	}

	public BigAdjacencyTable getInAdjacencyTable()
	{
		if (isBiDirectional())
		{
			return inEdges.get();
		}

		return null;
	}

	public GraphTopology load(DataSet<GraphTopology> dataset)
	{
		return load(dataset, false);
	}

	public GraphTopology load(DataSet<GraphTopology> dataset, boolean forceGlobalLoad)
	{
		if ( ! dataset.getAllocation().equals(getAllocation()))
			throw new IllegalArgumentException(
					"dataset does not have the same allocation than the graph");

		new OneNodeOneRequest<NoReturn>()
		{
			@Override
			protected ComputationRequest<NoReturn> createComputationRequestForNode(
					OctojusNode n)
			{
				LoadRequest job = new LoadRequest();
				job.datasetId = dataset.getID();
				job.topologyId = getID();
				job.forceGlobalLoad = forceGlobalLoad;
				return job;
			}
		}.execute(getAllocation().getNodes());

		return this;
	}

	@Override
	public void save()
	{
		new OneNodeOneRequest<NoReturn>()
		{
			@Override
			protected ComputationRequest<NoReturn> createComputationRequestForNode(
					OctojusNode n)
			{
				SaveRequest job = new SaveRequest();
				job.topologyId = getID();
				return job;
			}

		}.execute(getAllocation().getNodes());
	}

	public boolean isBiDirectional()
	{
		return biDirectional;
	}

	public boolean isUndirected()
	{
		return undirected;
	}
	
	public void setBiDirectional(boolean biDirectional)
	{
		setBiDirectional(biDirectional, false);
	}

	public void setBiDirectional(boolean biDirectional, boolean verbose)
	{
		if (isBiDirectional() == biDirectional)
			return; // No change
		String inEdgesId = getID() + IN_EDGES_SUFFIX;
		if (( ! isBiDirectional()) && biDirectional)
		{
			// Change from unidirectional to bidirectional
			this.inEdges = new BigObjectReference<BigAdjacencyTable>(inEdgesId);
			this.biDirectional = biDirectional;

			// the enclosed code will executed on the initiator node only
			if (getAllocation().getInitiatorNode().isLocalNode())
			{
				@SuppressWarnings("unused")
				BigAdjacencyTable reverse = outEdges.get().createReverseTable(inEdgesId,
						verbose);
			}
		}
		else
		{
			// Change from bidirectional to unidirectional
			if (getAllocation().getInitiatorNode().isLocalNode())
			{
				this.inEdges.get().delete();
			}
			this.inEdges = null;
			this.biDirectional = false;
		}
		if (getAllocation().getInitiatorNode().isLocalNode())
		{
			new OneNodeOneRequest<Boolean>()
			{
				@Override
				protected ComputationRequest<Boolean> createComputationRequestForNode(
						OctojusNode n)
				{
					SetBidirectionalRequest job = new SetBidirectionalRequest();
					job.topologyId = getID();
					job.biDirectional = biDirectional;
					job.inEdgesId = inEdgesId;
					return job;
				}
			}.execute(getAllocation().getNodes());
		}
	}

	public void addEdge(long src, long dst)
	{
		outEdges.get().add(src, dst);

		if (biDirectional)
		{
			inEdges.get().add(dst, src);
		}
	}

	@Override
	public void delete()
	{
		outEdges.get().delete();
		if (inEdges != null)
			inEdges.get().delete();
		super.delete();
	}

	@Override
	public void clearLocalData()
	{
	}

	@SuppressWarnings("serial")
	protected static class SetBidirectionalRequest extends ComputationRequest<Boolean>
	{
		String topologyId;
		boolean biDirectional;
		String inEdgesId;

		@Override
		protected Boolean compute() throws Throwable
		{
			GraphTopology topology = (GraphTopology) BigObjectRegistry.defaultRegistry
					.get(topologyId);
			assert topology != null;
			topology.setBiDirectional(biDirectional);
			if (biDirectional)
			{
				topology.inEdges = new BigObjectReference<BigAdjacencyTable>(inEdgesId);
			}
			else
			{
				topology.inEdges = null;
			}
			return biDirectional;
		}
	}

	public int getNumberOfLocalVertices()
	{
		return getOutAdjacencyTable().getLocalData().size();
	}

	public long getNumberOfVertices()
	{
		return getOutAdjacencyTable().getNumberOfVertices();
	}

	public long getNumberOfVertices(LongPredicate filter)
	{
		return getOutAdjacencyTable().getNumberOfVertices(filter);
	}

	public long getNumberOfEdges()
	{
		return getOutAdjacencyTable().getNumberOfEdges();
	}

	public Map<OctojusNode, Integer> getNumberOfVerticesPerNode()
	{
		return new OneNodeOneRequest<Integer>()
		{
			@Override
			protected ComputationRequest<Integer> createComputationRequestForNode(
					OctojusNode n)
			{
				return new NumberOfVerticesAlgorithm.NumberOfVerticesLocalExec(
						getOutAdjacencyTable().getID());
			}
		}.execute(getAllocation().getNodes());
	}

	public Map<OctojusNode, Long> getNumberOfOutEdgesPerNode()
	{
		return new OneNodeOneRequest<Long>()
		{
			@Override
			protected ComputationRequest<Long> createComputationRequestForNode(
					OctojusNode n)
			{
				return new NumberOfEdgesAlgorithm.NumberOfEdgesLocalExec(
						getOutAdjacencyTable().getID());
			}
		}.execute(getAllocation().getNodes());
	}

	public Map<OctojusNode, Long> getNumberOfInEdgesPerNode()
	{
		return new OneNodeOneRequest<Long>()
		{
			@Override
			protected ComputationRequest<Long> createComputationRequestForNode(
					OctojusNode n)
			{
				return new NumberOfEdgesAlgorithm.NumberOfEdgesLocalExec(
						getInAdjacencyTable().getID());
			}
		}.execute(getAllocation().getNodes());
	}

	public Map<OctojusNode, EdgeAndVertexCount> getNumberOfEdgesAndVerticesPerNode()
	{
		return getOutAdjacencyTable().__numberOfEdgesVerticesPerNode();
	}

	public double getOutAverageDegree()
	{
		return getOutAdjacencyTable().getAverageDegree();
	}

	public MaxDegreeAlgorithmResult getOutMaxDegree()
	{
		return getOutAdjacencyTable().getMaxDegree();
	}

	public MaxDegreeAlgorithmResult getOutMaxDegree(LongPredicate predicate)
	{
		return getOutAdjacencyTable().getMaxDegree(predicate);
	}

	@SuppressWarnings("serial")
	protected static class LoadRequest extends ComputationRequest<NoReturn>
	{
		String datasetId;
		String topologyId;
		boolean forceGlobalLoad;

		@Override
		protected NoReturn compute() throws Throwable
		{
			@SuppressWarnings("unchecked")
			DataSet<GraphTopology> dataset = (DataSet<GraphTopology>) BigObjectRegistry.defaultRegistry
					.get(datasetId);
			GraphTopology topology = (GraphTopology) BigObjectRegistry.defaultRegistry
					.get(topologyId);
			assert dataset != null;
			assert topology != null;
			BigAdjacencyTable outAdjacencyTable = topology.getOutAdjacencyTable();
			assert outAdjacencyTable != null;

			RegularFile globalFile = dataset.getFile();
			RegularFile outLocalFile = outAdjacencyTable.__getLocalFile();

			if (forceGlobalLoad)
			{
				globalLoad(dataset, topology, globalFile);
			}
			else if (topology.isBiDirectional())
			{
				BigAdjacencyTable inAdjacencyTable = topology.getInAdjacencyTable();
				RegularFile inLocalFile = inAdjacencyTable.__getLocalFile();

				if (outLocalFile.exists() && outLocalFile.isNewerThan(globalFile)
						&& inLocalFile.exists() && inLocalFile.isNewerThan(globalFile))
				{
					loadOutAdjacencyTable(outAdjacencyTable, outLocalFile);
					loadInAdjacencyTable(inAdjacencyTable, inLocalFile);
				}
				else
				{
					globalLoad(dataset, topology, globalFile);
				}
			}
			else
			{
				if (outLocalFile.exists() && outLocalFile.isNewerThan(globalFile))
				{
					loadOutAdjacencyTable(outAdjacencyTable, outLocalFile);
				}
				else
				{
					globalLoad(dataset, topology, globalFile);
				}
			}

			return null;
		}

		private void globalLoad(DataSet<GraphTopology> dataset, GraphTopology topology,
				RegularFile globalFile) throws IOException
		{
			// Global load from the file returned by dataset.getFile()
			System.out
					.println("Loading dataset from global file: " + globalFile.getPath());
			dataset.loadLocalPartTo(topology);

		}

		private void loadOutAdjacencyTable(BigAdjacencyTable outAdjacencyTable,
				RegularFile outLocalFile) throws ClassNotFoundException, IOException
		{
			System.out.println("Loading Out Adjacency Table from local file: "
					+ outLocalFile.getPath());
			outAdjacencyTable.__local_loadFromDisk();
		}

		private void loadInAdjacencyTable(BigAdjacencyTable inAdjacencyTable,
				RegularFile inLocalFile) throws ClassNotFoundException, IOException
		{
			System.out.println("Loading In Adjacency Table from local file: "
					+ inLocalFile.getPath());
			inAdjacencyTable.__local_loadFromDisk();
		}
	}

	@Override
	public void __local_loadFromDisk() throws IOException, ClassNotFoundException
	{
		getOutAdjacencyTable().__local_loadFromDisk();

		if (biDirectional)
		{
			getInAdjacencyTable().__local_loadFromDisk();
		}
	}

	@SuppressWarnings({ "serial" })
	protected static class SaveRequest extends ComputationRequest<NoReturn>
	{
		String topologyId;

		@Override
		protected NoReturn compute() throws Throwable
		{
			GraphTopology topology = (GraphTopology) BigObjectRegistry.defaultRegistry
					.get(topologyId);
			BigAdjacencyTable outAdjacencyTable = topology.getOutAdjacencyTable();
			assert outAdjacencyTable != null;
			outAdjacencyTable.__local__saveToDisk();

			if (topology.isBiDirectional())
			{
				BigAdjacencyTable inAdjacencyTable = topology.getInAdjacencyTable();
				assert inAdjacencyTable != null;
				inAdjacencyTable.__local__saveToDisk();
			}
			return null;
		}
	}

	static private int Counter = 0;

	@SuppressWarnings("unused")
	static private synchronized int incrAndGetCounter()
	{
		Counter++;
		return Counter;
	}

	public AssignConnectedComponents __getAssignConnectedComponents()
	{
		AssignConnectedComponents pr = (AssignConnectedComponents) BigObjectRegistry.defaultRegistry
				.get(getID() + "/AssignConnectedComponents");

		if (pr == null)
		{
			pr = new AssignConnectedComponents(this);
			pr.execute();
		}

		return pr;
	}

	public DistributionForLongs getConnectedComponentDistribution()
	{
		return new ConnectedComponentsSize(__getAssignConnectedComponents()).execute();
	}

	public long getNumberOfConnectedComponents()
	{
		return getConnectedComponentDistribution().getOccuringObjects().size();
	}

	public long getVertexInTheGreatestConnectedComponent()
	{
		return getConnectedComponentDistribution().getMostOccuringObject();
	}

	public LongSet getConnectedComponentRepresentatives()
	{
		return new EnumeratingConnectedComponents(__getAssignConnectedComponents())
				.execute();
	}

	public DiameterResult getDiameter_iFUB(LongPredicate vertexMatcher)
	{
		long src = twoSweeps(pickRandomVertex(new Random()), vertexMatcher);
		return getDiameter_iFUB(src, vertexMatcher);
	}

	public DiameterResult getDiameter_iFUB(long src, LongPredicate vertexMatcher)
	{
		List<IfubListener> listeners = new ArrayList<IfubListener>();

		if (IfubStdoutLogger.INSTANCE != null)
		{
			listeners.add(IfubStdoutLogger.INSTANCE);
		}

		return IFub.computeDiameter(this, src, vertexMatcher);
	}

	public long twoSweeps(long src, LongPredicate vertexMatcher)
	{
		long fartestVertex = getOutAdjacencyTable().computeFarthestVertex(src, vertexMatcher).key;
		BFSResult r = getOutAdjacencyTable().bfs(src, true, vertexMatcher);
		Path path = Path.getShortestPathTo(src, fartestVertex, r.predecessorMap);
		r.distanceDHT.delete();
		r.predecessorMap.delete();
		return path.getCenterVertex();
	}

	@Override
	public LongObjectOpenHashMap<LongSet> getLocalMap()
	{
		return getOutAdjacencyTable().getLocalData();
	}

	public LongSet getOutNeighbors(long v)
	{
		return getOutAdjacencyTable().get(v);
	}

	public LongSet getInNeighbors(long v)
	{
		return getInAdjacencyTable().get(v);
	}

	public boolean isMultigraph()
	{
		return multigraph;
	}

}
