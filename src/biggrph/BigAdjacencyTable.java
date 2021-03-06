/////////////////////////////////////////////////////////////////////////////////////////
// 
//                 Université de Nice Sophia-Antipolis  (UNS) - 
//                 Centre National de la Recherche Scientifique (CNRS)
//                 Copyright © 2015 UNS, CNRS All Rights Reserved.
// 
//     These computer program listings and specifications, herein, are
//     the property of Université de Nice Sophia-Antipolis and CNRS
//     shall not be reproduced or copied or used in whole or in part as
//     the basis for manufacture or sale of items without written permission.
//     For a license agreement, please contact:
//     <mailto: licensing@sattse.com> 
//
//
//
//     Author: Luc Hogie – Laboratoire I3S - luc.hogie@unice.fr
//
//////////////////////////////////////////////////////////////////////////////////////////

package biggrph;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import com.carrotsearch.hppc.LongByteMap;
import com.carrotsearch.hppc.LongByteOpenHashMap;
import com.carrotsearch.hppc.LongContainer;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;

import biggrph._algo.count.EdgeAndVertexCount;
import biggrph._algo.count.NeighborhoodContainerClassDistribution;
import biggrph._algo.count.NumberOfEdgesAlgorithm;
import biggrph._algo.count.NumberOfNeighborlessVertices;
import biggrph._algo.count.NumberOfVerticesAlgorithm;
import biggrph._algo.count.NumberOfVerticesAndEdgesPerNode;
import biggrph._algo.count.NumberOfVerticesPerNodeAlgorithm;
import biggrph._algo.locality.ActualEdgeLocalityAlgorithm;
import biggrph._algo.locality.EdgeLocalityInfo;
import biggrph.algo.degree.DegreeDistribution;
import biggrph.algo.degree.MaxDegreeAlgorithm;
import biggrph.algo.degree.MaxDegreeAlgorithmResult;
import biggrph.algo.search.bfs.BFS;
import biggrph.algo.search.bfs.BFS.BFSResult;
import biggrph.algo.topology_generator.Grid;
import biggrph.algo.topology_generator.RandomGraph;
import biggrph.algo.topology_generator.ReverseNavigationEnabler;
import biggrph.algo.topology_generator.Undirectionalizer;
import bigobject.Service;
import dht.DHTAllocation;
import dht.KeyValuePair;
import dht.ObjectDHT;
import dht.ObjectDHTKeyCounter;
import dht.ObjectDHTKeyMatcher;
import dht.SerializableLongObjectMap;
import dht.allocation.BasicHashDHTAllocation;
import dht.set.ArrayListLongSet32;
import dht.set.ArrayListLongSet64;
import dht.set.ArrayLongSet32;
import dht.set.ArrayLongSet64;
import dht.set.HashLongSet32;
import dht.set.HashLongSet64;
import dht.set.LongSet;
import dht.set.LongSet32;
import dht.set.LongSets;
import ldjo.bsp.BSPable;
import octojus.OctojusNode;
import toools.collection.bigstuff.longset.LongCursor;
import toools.io.BinaryReader;
import toools.io.DataBinaryEncoding;
import toools.io.FullDuplexDataConnection2;
import toools.io.TextNumberReader;
import toools.io.file.RegularFile;
import toools.math.Distribution;
import toools.math.DistributionForLongs;
import toools.math.IntegerSequenceAnalyzer;
import toools.net.NetUtilities;
import toools.net.TCPConnection;
import toools.text.TextUtilities;
import toools.util.LongPredicate;

public class BigAdjacencyTable extends ObjectDHT<LongSet> implements BSPable
{
	static
	{
		if (BigGrphWebNotification.enabled)
		{
			NetUtilities.notifyUsage(
					"http://www.i3s.unice.fr/~hogie/software/register_use.php",
					"biggrph");
		}
	}

	private boolean multigraph;

	public BigAdjacencyTable(long initialCapacity, boolean multigraph)
	{
		this("biggrph-" + System.currentTimeMillis(),
				new BasicHashDHTAllocation(BigGrphCluster.localhost()), initialCapacity,
				multigraph);
	}

	public BigAdjacencyTable(String id, DHTAllocation a, long initialCapacity,
			boolean multigraph)
	{
		super(id, a, initialCapacity, multigraph);
		this.multigraph = multigraph;
	}

	public Iterable<LongCursor> getLocalVertices2()
	{
		return LongCursor.fromHPPC(getLocalData().keys());
	}

	public Iterator<LongCursor> getLocalVerticesIterator()
	{
		return getLocalVertices2().iterator();
	}

	public LongContainer getLocalVertices()
	{
		return getLocalData().keys();
	}

	public Iterator<LongCursor> getNeighborhoodIterator(long vertex)
	{
		return getNeighborhood(vertex).iterator();
	}

	public LongSet getNeighborhood(long vertex)
	{
		return get(vertex);
	}

	// public AssignConnectedComponents __getAssignConnectedComponents()
	// {
	// AssignConnectedComponents pr = (AssignConnectedComponents)
	// BigObjectRegistry.defaultRegistry
	// .get(getID() + "/AssignConnectedComponents");
	//
	// if (pr == null)
	// {
	// pr = new AssignConnectedComponents(this);
	// pr.execute();
	// }
	//
	// return pr;
	// }

	/**
	 * Sort all entries that are organized as a sequence (array or array list)
	 */
	public void sortArrays()
	{
		for (ObjectCursor<LongSet> n : getLocalData().values())
		{
			if (n.value instanceof ArrayLongSet64)
			{
				Arrays.sort(((ArrayLongSet64) n.value).array);
			}
			else if (n.value instanceof ArrayLongSet32)
			{
				Arrays.sort(((ArrayLongSet32) n.value).array);
			}
			else if (n.value instanceof ArrayListLongSet64)
			{
				((ArrayListLongSet64) n.value).sort();
			}
		}
	}

	public void convertToArray(boolean sort)
	{
		for (LongCursor v : getLocalVertices2())
		{
			LongSet ne = get(v.value);
			LongSet convertedSet;

			if (ne instanceof HashLongSet64)
			{
				convertedSet = new ArrayLongSet64(((ArrayLongSet64) ne).toArray());
			}
			else if (ne instanceof HashLongSet32)
			{
				convertedSet = new ArrayLongSet32(((HashLongSet32) ne).toArray());
			}
			else if (ne instanceof ArrayListLongSet64)
			{
				convertedSet = new ArrayLongSet64(((ArrayLongSet64) ne).toArray());
			}
			else
			{
				throw new IllegalStateException(
						"cannot convert " + ne.getClass() + " to array");
			}

			getLocalData().put(v.value, convertedSet);
		}
	}

	public void ensureExists(long e)
	{
		if (isLocalElement(e))
		{
			__local_ensureExists(e);
		}
		else
		{
			boolean sync = true;
			TCPConnection c = connectTo(ensureExists, e);
			c.out.writeLong2(e);
			c.out.writeBoolean2(sync);

			if (sync)
			{
				boolean b = c.in.readBoolean2();
				assert b;
			}
		}
	}

	Service ensureExists = new Service()
	{
		@Override
		public void serveThrough(FullDuplexDataConnection2 c)
		{
			long e = c.in.readLong2();
			__local_ensureExists(e);

			// if ACK requested (if sync mode requested)
			if (c.in.readBoolean2())
			{
				c.out.writeBoolean2(true);
			}
		}
	};

	// public long getCenter_2sweeps(Random r)
	// {
	// long v = pickRandomVertex(r);
	// return twoSweeps(v);
	// }

	// public long twoSweeps(long src)
	// {
	// long fartestVertex = computeFarthestVertex(src).key;
	// DijkstraDistanceAndPredecessorAssigner dijkstra = dijkstra(src);
	// fartestVertex =
	// GreatestValueFinder.findGreatestValue(dijkstra.getDistancesMap(),
	// src).key;
	// Path path = dijkstra.getShortestPathTo(fartestVertex);
	// return path.array[path.array.length / 2];
	// }

	public synchronized LongSet __local_ensureExists(long v)
	{
		LongSet set = getLocalData().get(v);

		if (set == null)
		{
			super.set(v, set = LongSets.emptySet);
		}

		return set;
	}

	public synchronized LongSet __local_ensureFit(long v, boolean require64set,
			int additiveCapacity)
	{
		LongSet set = getLocalData().get(v);

		// if there we no values initially, simply creates an empty expandable
		// set
		if (set == null || set == LongSets.emptySet)
		{
			set = createSet(require64set, additiveCapacity);
			getLocalData().put(v, set);
		}
		// upgrade a 32-bit set to 64-bit
		else if (require64set && ! set.is64bit())
		{
			LongSet newset = createSet(require64set, set.size() + additiveCapacity);
			newset.addAll(set);
			getLocalData().put(v, set = newset);
		}

		return set;
	}

	private LongSet createSet(boolean require64bits, int capacity)
	{
		if (require64bits)
		{
			if (multigraph)
			{
				return new ArrayListLongSet64(capacity);
			}
			else
			{
				return new HashLongSet64(capacity);
			}
		}
		else
		{
			if (multigraph)
			{
				return new ArrayListLongSet32(capacity);
			}
			else
			{
				return new HashLongSet32(capacity);
			}
		}
	}

	synchronized public void __local_add(long src, long dest)
	{
		LongSet s = __local_ensureFit(src, dest >= Integer.MAX_VALUE, 1);
		s.add(dest);
	}

	public void add(long src, long dest)
	{
		if (isLocalElement(src))
		{
			__local_add(src, dest);
		}
		else
		{
			TCPConnection c = connectTo(add, src);

			// that's not a sync request, that an "add"
			c.out.writeBoolean2(false);
			c.out.writeLong2(src);
			c.out.writeLong2(dest);
		}
	}

	public void ensureRemoteAddsHaveCompleted()
	{
		for (OctojusNode node : getAllocation().getNodes())
		{
			TCPConnection c = connectTo(add, node);
			c.out.writeBoolean2(true);
			c.in.readBoolean2();
		}
	}

	Service add = new Service()
	{

		@Override
		public void serveThrough(FullDuplexDataConnection2 c)
		{
			boolean sync = c.in.readBoolean2();

			// the client simply want to sync its previous "add"s.
			if (sync)
			{
				c.out.writeBoolean2(true);
			}
			else
			{
				long src = c.in.readLong2();
				long dest = c.in.readLong2();
				__local_add(src, dest);
			}
		}
	};

	@Override
	public void set(long v, LongSet s)
	{
		if (s == null)
			throw new NullPointerException();

		super.set(v, s);
	}

	// public long getEccentricity(long v)
	// {
	// return computeFarthestVertex(v).value;
	// }

	public void removeIncidence(long src, long dest)
	{
		if (isLocalElement(src))
		{
			__local_remove(src, dest);
		}
		else
		{
			TCPConnection c = connectTo(remove, src);
			c.out.writeLong2(src);
			c.out.writeLong2(dest);
		}
	}

	Service remove = new Service()
	{

		@Override
		public void serveThrough(FullDuplexDataConnection2 c)
		{
			long src = c.in.readLong2();
			long dest = c.in.readLong2();
			__local_remove(src, dest);
		}
	};

	private void __local_remove(long src, long dest)
	{
		getLocalData().get(src).removeAllOccurences(dest);
	}

	public long getNumberOfVertices()
	{
		return NumberOfVerticesAlgorithm.compute(this);
	}

	public long getNumberOfVertices(LongPredicate filter)
	{
		return NumberOfVerticesAlgorithm.compute(this, filter);
	}

	public long getNumberOfEdges()
	{
		return NumberOfEdgesAlgorithm.compute(this);
	}

	public double getAverageDegree()
	{
		long nbVertices = getNumberOfVertices();
		assert nbVertices <= Double.MAX_VALUE;
		long nbEdges = getNumberOfEdges();
		assert nbEdges <= Double.MAX_VALUE;
		return nbEdges / (double) nbVertices;
	}

	public EdgeLocalityInfo __getEdgeLocality()
	{
		return ActualEdgeLocalityAlgorithm.compute(this);
	}

	public Map<OctojusNode, Integer> __numberOfVerticesPerNode()
	{
		return NumberOfVerticesPerNodeAlgorithm.compute(this);
	}

	public Map<OctojusNode, EdgeAndVertexCount> __numberOfEdgesVerticesPerNode()
	{
		return NumberOfVerticesAndEdgesPerNode.compute(this);
	}

	public DistributionForLongs getDegreeDistribution()
	{
		return new DegreeDistribution(this).execute();
	}

	// public DistributionForLongs getDistanceDistribution(long src)
	// {
	// DijkstraDistanceAndPredecessorAssigner ass =
	// DijkstraDistanceAndPredecessorAssigner
	// .compute(this, src);
	// return new DistanceDistribution(ass.getDistancesMap()).execute();
	// }

	public MaxDegreeAlgorithmResult getMaxDegree()
	{
		return new MaxDegreeAlgorithm(this).execute();
	}

	public MaxDegreeAlgorithmResult getMaxDegree(LongPredicate predicate)
	{
		return new MaxDegreeAlgorithm(this, predicate).execute();
	}

	public long getNumberOfIsolatedVertices()
	{
		return NumberOfNeighborlessVertices.compute(this);
	}

	// public Path getShortestPath(long src, long dest)
	// {
	// DijkstraDistanceAndPredecessorAssigner assigner =
	// DijkstraDistanceAndPredecessorAssigner
	// .compute(this, src);
	// System.out.println(assigner);
	// return assigner.getShortestPathTo(dest);
	// }

	// public DistributionForLongs getConnectedComponentDistribution()
	// {
	// return new
	// ConnectedComponentsSize(__getAssignConnectedComponents()).execute();
	// }
	//
	// public long getNumberOfConnectedComponents()
	// {
	// return getConnectedComponentDistribution().getOccuringObjects().size();
	// }
	//
	// public long getVertexInTheGreatestConnectedComponent()
	// {
	// return getConnectedComponentDistribution().getMostOccuringObject();
	// }
	//
	// public LongSet getConnectedComponentRepresentatives()
	// {
	// return new
	// EnumeratingConnectedComponents(__getAssignConnectedComponents())
	// .execute();
	// }

	// public float getRankOfVertex(long v)
	// {
	// PageRankAssigner pr = new PageRankAssigner(this, new
	// NCoresNThreadsPolicy());
	// pr.execute();
	// return pr.computedRanks.get(v);
	// }

	public void grid(int width, int height, boolean directed, boolean createDiags)
	{
		Grid.grid(this, width, height, directed, createDiags);
	}

	public void randomize(long size, int minDegree, int maxDegree, Random prng)
	{
		RandomGraph.createIncidences(this, size, minDegree, maxDegree, prng);
	}

	// private Long diameter;

	// public long getDiameter_nBFS()
	// {
	// if (diameter == null)
	// {
	// diameter = NBFSDiameter.computeDiameter(this);
	// }
	//
	// return diameter;
	// }

	// public DiameterResult getDiameter_iFUB()
	// {
	// long src = twoSweeps(pickRandomVertex(new Random()));
	// return getDiameter_iFUB(src);
	// }

	// public DiameterResult getDiameter_iFUB(long src)
	// {
	// List<IfubListener> listeners = new ArrayList<IfubListener>();
	//
	// if (IfubStdoutLogger.INSTANCE != null)
	// {
	// listeners.add(IfubStdoutLogger.INSTANCE);
	// }
	//
	// return IFub.computeDiameter(this, src);
	// }

	// public long getDiameter_4sweeps(Random r)
	// {
	// long center2sweeps = getCenter_2sweeps(r);
	// long border = computeFarthestVertex(center2sweeps).key;
	// return computeFarthestVertex(border).value;
	//
	// }

	@SuppressWarnings("rawtypes")
	public Distribution<Class> __computeSetTypeDistribution()
	{
		return new NeighborhoodContainerClassDistribution(this).execute();
	}

	public Map<OctojusNode, Long> __getMemoryFootprintInBytesPerNode()
	{
		return new MemoryFootPrintInBytes(this).execute();
	}

	public long __getMemoryFootprintInBytes()
	{
		IntegerSequenceAnalyzer a = new IntegerSequenceAnalyzer();
		a.addLongs(new MemoryFootPrintInBytes(this).execute().values());
		return a.getSum();
	}

	public long __local_getMemoryFootprintInBytes()
	{
		long r = 0;

		for (ObjectCursor<LongSet> c : getLocalData().values())
		{
			r += c.value.getMemoryFootprintInBytes();
		}

		return r;
	}

	@Override
	public void __local__saveToDisk() throws IOException
	{
		if ( ! __getLocalFile().getParent().exists())
		{
			__getLocalFile().getParent().mkdirs();
		}

		OutputStream dos = __getLocalFile().createWritingStream();
		System.out.println("writing " + getLocalData().keys().size() + " entries");
		writeInt(dos, getLocalData().keys().size());

		for (LongCursor c : getLocalVertices2())
		{
			long v = c.value;
			writeLong(dos, v);
			LongSet nei = get(v);
			writeInt(dos, nei.size());
			boolean is32bit = nei instanceof LongSet32;
			writeBoolean(dos, is32bit);

			if (is32bit)
			{
				for (LongCursor nc : nei)
				{
					writeInt(dos, (int) nc.value);
				}
			}
			else
			{
				for (LongCursor nc : nei)
				{
					writeLong(dos, nc.value);
				}
			}
		}

		dos.close();
	}

	static byte[] buf = new byte[8];

	private static void writeLong(OutputStream os, long v) throws IOException
	{
		DataBinaryEncoding.writeLong(v, buf, 0);
		os.write(buf, 0, 8);
	}

	private static void writeInt(OutputStream out, int v) throws IOException
	{
		DataBinaryEncoding.writeInt(v, buf, 0);
		out.write(buf, 0, 4);
	}

	private static void writeBoolean(OutputStream os, boolean v) throws IOException
	{
		os.write(v ? 1 : 0);
	}

	@Override
	public void __local_loadFromDisk() throws IOException, ClassNotFoundException
	{

		System.out
				.println("loading dataset from local file " + __getLocalFile().getPath());

		InputStream is = __getLocalFile().createReadingStream();
		BinaryReader br = new BinaryReader(is, 65536 * 256);
		final int numberOfEntries = br.nextInt();
		SerializableLongObjectMap<LongSet> map = new SerializableLongObjectMap<LongSet>(
				numberOfEntries);
		setLocalData(map);

		for (long i = 0; i < numberOfEntries; ++i)
		{
			if (i > 0 && i % 1000000 == 0)
				System.out.println(TextUtilities.toHumanString(i) + " vertices loaded.");

			long v = br.nextLong();
			int n = br.nextInt();
			boolean is32bit = br.nextBoolean();

			if (is32bit)
			{
				int[] neighbors = new int[n];

				for (int j = 0; j < n; ++j)
				{
					neighbors[j] = br.nextInt();
				}

				map.put(v, new ArrayLongSet32(neighbors));
				// map.put(v, new HashLongSet32(neighbors));
			}
			else
			{
				long[] neighbors = new long[n];

				for (int j = 0; j < n; ++j)
				{
					neighbors[j] = br.nextLong();
				}

				map.put(v, new ArrayLongSet64(neighbors));
				// map.put(v, new HashLongSet64(neighbors));
			}
		}

		System.out.println(
				TextUtilities.toHumanString(numberOfEntries) + " vertices loaded.");
		is.close();
	}

	public LongObjectOpenHashMap<LongSet>.KeysContainer __local_getVertices()
	{
		return getLocalData().keys();
	}

	// public boolean __local_isIs32bits()
	// {
	// return is32bits;
	// }

	final LongByteMap nodeLocationMap = new LongByteOpenHashMap();

	public void readPartitionFile(RegularFile partitionFile) throws IOException
	{
		System.out.println("Reading partition file " + partitionFile);
		InputStream is = partitionFile.createReadingStream();
		TextNumberReader s = new TextNumberReader(is, 65536 * 256);
		int numberOfEdgesParsed = 0;
		@SuppressWarnings("unused")
		int numberOfEdgesAdded = 0;

		while (s.hasNext())
		{
			if (numberOfEdgesParsed > 0 && numberOfEdgesParsed % 1000000 == 0)
			{
				System.out.println(TextUtilities.toHumanString(numberOfEdgesParsed)
						+ " vertices located");
			}

			numberOfEdgesParsed++;

			long v = s.nextLong();
			long nodeID = s.nextLong();
			assert nodeID < 128;
			nodeLocationMap.put(v, (byte) nodeID);
		}
	}

	public KeyValuePair<Long> computeFarthestVertex(long src, LongPredicate vertexMatcher)
	{
		BFSResult r = bfs(src, false, vertexMatcher);
		KeyValuePair<Long> p = r.distanceDHT.getGreatestValue();
		r.distanceDHT.delete();
		return p;
	}

	public BFSResult bfs(long src, boolean computePredecessors, LongPredicate driver)
	{
		return BFS.compute(this, src, computePredecessors, driver);
	}

	// public BFSDistanceAssigner bfs(long src, BSPListener<EmptyMessage,
	// NoReturn> listener)
	// {
	// return bfs(src, listener, new NCoresNThreadsPolicy());
	// }

	// public LongDHT bfs(long src, BSPListener<EmptyMessage, NoReturn>
	// listener,
	// MultiThreadPolicy p)
	// {
	// return BFSDistanceAssigner.compute(this, src, listener, p)
	// .getDistancesDistributedMap();
	//
	// }

	// public BFSDistanceAssigner bfs(long src,
	// BSPListener<EmptyMessage, NoReturn> listener, MultiThreadPolicy p)
	// {
	// BFSDistanceAssigner ass = BFSDistanceAssigner.compute(this, src,
	// listener, p);
	// return ass;
	// }

	// public DijkstraDistanceAndPredecessorAssigner dijkstra(long src)
	// {
	// return DijkstraDistanceAndPredecessorAssigner.compute(this, src, null);
	// }

	public BigAdjacencyTable createReverseTable(String id)
	{
		return ReverseNavigationEnabler.createReverseTable(this, id);
	}

	public BigAdjacencyTable createReverseTable(String id, boolean verbose)
	{
		return ReverseNavigationEnabler.createReverseTable(this, id, verbose);
	}

	public void undirectionalize()
	{
		Undirectionalizer.indirect(this);
	}

	@Override
	public LongObjectOpenHashMap<LongSet> getLocalMap()
	{
		return getLocalData();
	}

	public long count(ObjectDHTKeyMatcher<LongSet> m)
	{
		return ObjectDHTKeyCounter.count(this, m);
	}

	public boolean isMultigraph()
	{
		return multigraph;
	}

}
