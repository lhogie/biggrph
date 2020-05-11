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
//			   Nicolas Chleq - INRIA
//
//////////////////////////////////////////////////////////////////////////////////////////

package biggrph.algo.cc;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import octojus.ComputationRequest;
import octojus.NoReturn;
import octojus.OctojusNode;
import octojus.OneNodeOneRequest;
import toools.StopWatch;
import toools.StopWatch.UNIT;
import toools.collection.bigstuff.longset.LongCursor;
import toools.io.FullDuplexDataConnection2;
import toools.net.TCPConnection;
import biggrph.BigAdjacencyTable;
import biggrph.GraphTopology;
import bigobject.Allocation;
import bigobject.BigObjectComputingRequest;
import bigobject.BigObjectRegistry;
import bigobject.LiveDistributedObject;
import bigobject.Service;

import com.carrotsearch.hppc.LongByteOpenHashMap;
import com.carrotsearch.hppc.LongFloatMap;
import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.cursors.LongObjectCursor;

import dht.DHTAllocation;
import dht.FloatDHT;
import dht.set.HashLongSet32;
import dht.set.HashLongSet64;
import dht.set.LongSet;
import dht.set.LongSet32;
import dht.set.LongSet64;

public class ClusteringCoefficient extends
		LiveDistributedObject<Serializable, Serializable>
{
	private BigAdjacencyTable adjTable;
	private boolean verbose;
	private boolean parallelExecution = false;
	private int nThreads = 1;

	public ClusteringCoefficient(GraphTopology topology)
	{
		this(topology.getOutAdjacencyTable(), false, 1);
	}

	public ClusteringCoefficient(GraphTopology topology, boolean verbose)
	{
		this(topology.getOutAdjacencyTable(), verbose, 1);
	}

	public ClusteringCoefficient(GraphTopology topology, boolean verbose, int nThreads)
	{
		this(topology.getOutAdjacencyTable(), verbose, nThreads);
	}

	private ClusteringCoefficient(BigAdjacencyTable adjTable, boolean verbose,
			int nThreads)
	{
		this(adjTable.getID() + "/ClusteringCoefficientAlgo", adjTable.getAllocation(),
				adjTable.getID(), verbose, nThreads);
	}

	protected ClusteringCoefficient(String id, Allocation allocation, String adjTableId,
			Boolean verbose, Integer nThreads)
	{
		super(id, allocation, adjTableId, verbose, nThreads);
		this.adjTable = (BigAdjacencyTable) BigObjectRegistry.defaultRegistry
				.get(adjTableId);
		this.verbose = verbose;
		this.nThreads = nThreads;
		this.parallelExecution = ((nThreads > 1) && (adjTable.getLocalData().size() > 10 * nThreads));
	}

	/*
	 * The algorithm needs to compute intersections of vertex neighborhood Some
	 * vertices are local and their neighbors are stored locally in an adjacency
	 * list, while other are located on another node of the cluster. We use a
	 * cache for the adjacency lists of these non-local vertices.
	 */
	private LongObjectOpenHashMap<LongSet> neighborCache;

	private ReentrantLock neighborCacheLock = new ReentrantLock();
	private Condition cacheEntryAdded = neighborCacheLock.newCondition();
	private Condition cacheEntryRemoved = neighborCacheLock.newCondition();

	private int cacheMaxEntries = 0;

	private LongIntOpenHashMap cacheAccessOps;

	/*
	 * While building the above cache, we keep track of the number of
	 * occurrences of a non-local vertex, in order to request its adjacency list
	 * only one time, when it is first encountered.
	 */
	private LongByteOpenHashMap remoteVertexState;
	static private byte NOTINCACHE = 0;
	static private byte REQUESTED = 1;
	static private byte INCACHE = 2;

	/*
	 * We keep track of the number of occurrences of a non-local vertex, in
	 * order to know how many times is will be requested from the cache.
	 */
	private LongIntOpenHashMap vertexOccurrences;
	private AtomicInteger localEdgeCount = new AtomicInteger(0);
	private AtomicInteger remoteEdgeCount = new AtomicInteger(0);

	private volatile boolean cacheFilled = false;
	private int cacheMisses = 0;

	/*
	 * The intersection of two adjacency lists performs better when at least one
	 * of the adjacency list is an HashSet where the test of existence in the
	 * adjacency list can be performed in quasi-constant time. One of the list
	 * is used to iterate, the other is used to test the existence of an element
	 * in it. If the graph is loaded from an edge-list file or an adjacency-list
	 * file, all the adjacency lists are stored using HashLongSet32 or
	 * HashLongSet64 instances : this is the easy case. If the graph is loaded
	 * from a serialized binary file, the adjacency lists are stored using
	 * ArrayLongSet32 or ArrayLongSet64, where the contains() function does not
	 * return in constant time. For long adjacency lists we substitute them with
	 * an HashSet with the same content and this new set is used for the
	 * instersection operations. The following structure is a cache of all these
	 * subsituted lists.
	 */
	private LongObjectOpenHashMap<LongSet> substitutedNeigbors;
	/*
	 * Because the accesses on the previous cache are multithreaded we use a
	 * rw-lock to protect it.
	 */
	final ReentrantReadWriteLock substitutionLock = new ReentrantReadWriteLock();

	/**
	 * Main entry point.
	 * 
	 * @return
	 */
	public FloatDHT execute()
	{
		FloatDHT result = new FloatDHT(adjTable.getID() + "/clusteringCoefficients",
				(DHTAllocation) getAllocation(), adjTable.getNumberOfVertices());
		return execute(result);
	}

	/**
	 * Second entry point where the results are stored in the DHT given as
	 * argument.
	 * 
	 * @param results
	 * @return
	 */
	public FloatDHT execute(FloatDHT results)
	{
		new OneNodeOneRequest<NoReturn>()
		{
			@Override
			protected ComputationRequest<NoReturn> createComputationRequestForNode(
					OctojusNode n)
			{
				return new CCExecutionRequest(getID(), results.getID());
			}
		}.execute(getAllocation().getNodes());
		return results;
	}

	@SuppressWarnings("serial")
	static private class CCExecutionRequest extends
			BigObjectComputingRequest<ClusteringCoefficient, NoReturn>
	{
		String outputId;

		public CCExecutionRequest(String bigObjectID, String outputID)
		{
			super(bigObjectID);
			this.outputId = outputID;
		}

		@Override
		protected NoReturn localComputation(ClusteringCoefficient cc) throws Throwable
		{
			FloatDHT result = (FloatDHT) BigObjectRegistry.defaultRegistry.get(outputId);
			cc.localExecute(result);
			return null;
		}
	}

	private ExecutorService executor = null;

	public FloatDHT localExecute(FloatDHT result)
	{
		StopWatch sw = new StopWatch(UNIT.ms);

		int nLocalVertices = adjTable.getLocalMap().size();
		vertexOccurrences = new LongIntOpenHashMap(nLocalVertices);

		countRemoteOccurrences();

		int nRemoteVertices = vertexOccurrences.size();
		neighborCache = new LongObjectOpenHashMap<LongSet>(nRemoteVertices);
		remoteVertexState = new LongByteOpenHashMap(nRemoteVertices);
		cacheAccessOps = new LongIntOpenHashMap(nRemoteVertices);
		substitutedNeigbors = new LongObjectOpenHashMap<LongSet>(nLocalVertices);

		if (verbose)
		{
			System.out.println("Graph distribution: " + nLocalVertices
					+ " local vertices, " + nRemoteVertices + " remote vertices, "
					+ localEdgeCount.get() + " local edges, " + remoteEdgeCount.get()
					+ " remote edges.");
		}

//		String logFileName = getAllocation().getLocalNode().toString() + "-lccspeeds.dat";
//		PrintStream mlogFile = null;
//		try
//		{
//			mlogFile = new PrintStream(logFileName);
//		}
//		catch (FileNotFoundException e1)
//		{
//			e1.printStackTrace();
//		}
//		final PrintStream logFile = mlogFile;
		ScheduledExecutorService controlExecutor = Executors
				.newSingleThreadScheduledExecutor();
		controlExecutor.scheduleAtFixedRate(new Runnable()
		{
			@Override
			public void run()
			{
				evaluateProgressSpeed(null, false);
			}
		}, 0, 200, TimeUnit.MILLISECONDS);

		Thread fillCacheThread = new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				try
				{
					fillCache();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		}, "FillCacheThread");
		cacheFilled = false;
		fillCacheThread.start();

		if (parallelExecution)
		{
			if (verbose)
			{
				System.out.println("Executing ClusteringCoefficient in parallel with "
						+ nThreads + " threads.");
			}
			executor = Executors.newFixedThreadPool(nThreads);
			Iterator<LongObjectCursor<LongSet>> iterators[] = adjTable.getLocalData()
					.split(nThreads);
			for (int i = 0; i < nThreads; i++)
			{
				final Iterator<LongObjectCursor<LongSet>> iterator = iterators[i];
				Runnable task = new Runnable()
				{
					@Override
					public void run()
					{
						localExecute(iterator, result);
					}
				};
				executor.execute(task);
			}
			executor.shutdown();
			boolean terminated = false;
			while ( ! terminated)
			{
				try
				{
					terminated = executor.awaitTermination(1, TimeUnit.SECONDS);
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}
		else
		{
			Iterator<LongObjectCursor<LongSet>> iterator = adjTable.getLocalData()
					.iterator();
			localExecute(iterator, result);
		}
		try
		{
			fillCacheThread.join();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}

		controlExecutor.shutdown();
//		mlogFile.close();

		long elapsedTime = sw.getElapsedTime();
		if (verbose)
		{
			System.out.println("Clustering Coefficients: processed "
					+ processedEdgeCount.get() + " edges in " + elapsedTime
					+ "ms, cache miss: " + cacheMisses + ", final cache size: "
					+ neighborCache.size());
		}
		neighborCache.clear();
		substitutedNeigbors.clear();
		return result;
	}

	private AtomicInteger processedEdgeCount = new AtomicInteger(0);

	private int localExecute(Iterator<LongObjectCursor<LongSet>> iterator, FloatDHT result)
	{
		StopWatch sw = new StopWatch(UNIT.s);
		boolean prevPrintProgress = false;
		int vertexCount = 0;
		LongFloatMap resMap = result.getLocalMap();
		while (iterator.hasNext())
		{
			LongObjectCursor<LongSet> cursor = iterator.next();
			long src = cursor.key;
			vertexCount += 1;
			LongSet srcNeighbors = getLocalAdjacencyList(src, cursor.value);
			synchronized (resMap)
			{
				resMap.put(src, 0);
			}
			int srcDegree = srcNeighbors.size();
			if (srcDegree > 1)
			{
				for (LongCursor nelt : srcNeighbors)
				{
					long dst = nelt.value;
					processedEdgeCount.incrementAndGet();
					LongSet dstNeighbors = getAdjacencyList(dst);

					if (dstNeighbors != null && dstNeighbors.size() > 0)
					{
						int count = intersectionSize(srcNeighbors, dstNeighbors, src, dst);
						synchronized (resMap)
						{
							resMap.put(src, resMap.get(src) + count);
						}
					}
				}
				synchronized (resMap)
				{
					resMap.put(src, resMap.get(src) / (srcDegree * (srcDegree - 1)));
				}
				long elapsedTime = sw.getElapsedTime();
				if (verbose)
				{
					prevPrintProgress = printProgress(prevPrintProgress, vertexCount,
							processedEdgeCount.get(), elapsedTime);
				}
			}
		}
		return processedEdgeCount.get();
	}

	private boolean printProgress(boolean prevPrintProgress, int vertexCount,
			int edgeCount, long elapsedTime)
	{

		boolean printProgress = false;
		// Every 30 minutes after 1 hour of runtime
		if (elapsedTime >= 3600 && elapsedTime % 1800 == 0)
			printProgress = true;
		// or every 10 minutes after 10 minutes of runtime
		else if (elapsedTime >= 600 && elapsedTime % 600 == 0)
			printProgress = true;
		// or else every 2 minutes after 2 minutes of runtime
		else if (elapsedTime >= 120 && elapsedTime % 120 == 0)
			printProgress = true;
		if ( ! prevPrintProgress && printProgress)
		{
			System.out.println("Clustering Coefficients: processed " + vertexCount
					+ " vertices and " + edgeCount + " edges in " + elapsedTime
					+ " sec. Cache size = " + neighborCache.size() + " entries.");
		}
		return printProgress;
	}

	// Count the size of the intersection between srcNeighbors and dstNeighbors
	// ignoring both src and dst vertices
	private int intersectionSize(LongSet srcNeighbors, LongSet dstNeighbors, long src,
			long dst)
	{
		int count = 0;
		if ((srcNeighbors instanceof HashLongSet32 || srcNeighbors instanceof HashLongSet64)
				&& (dstNeighbors instanceof HashLongSet32 || dstNeighbors instanceof HashLongSet64))
		{
			LongSet smallest, largest;
			if (srcNeighbors.size() < dstNeighbors.size())
			{
				smallest = srcNeighbors;
				largest = dstNeighbors;
			}
			else
			{
				smallest = dstNeighbors;
				largest = srcNeighbors;
			}
			for (LongCursor cr : smallest)
			{
				long vertex = cr.value;
				if (vertex != src && vertex != dst && largest.contains(vertex))
				{
					count++;
				}
			}
		}
		else
		{
			for (LongCursor srcCr : srcNeighbors)
			{
				long srcNeighbor = srcCr.value;
				if (srcNeighbor != src && srcNeighbor != dst
						&& dstNeighbors.contains(srcNeighbor))
				{
					count++;
				}
			}
		}
		return count;
	}

	private void countRemoteOccurrences()
	{
		for (LongObjectCursor<LongSet> elt : adjTable.getLocalMap())
		{
			LongSet srcNeighbors = elt.value;
			for (LongCursor nelt : srcNeighbors)
			{
				long dst = nelt.value;
				if (adjTable.getAllocation().isLocalElement(dst))
				{
					localEdgeCount.incrementAndGet();
				}
				else
				{
					remoteEdgeCount.incrementAndGet();
					vertexOccurrences.put(dst, vertexOccurrences.get(dst) + 1);
				}
			}
		}
	}

	private ConcurrentHashMap<OctojusNode, LongOpenHashSet> hostTable = new ConcurrentHashMap<OctojusNode, LongOpenHashSet>();
	private int vertexSetCapacity = 1000;
	private int vertexSetTransmitThreshold = 1000;

	static final private boolean useSendThreadPool = true;

	private Map<OctojusNode, ExecutorService> sendThreadPool = null;

	private AtomicInteger cacheProcessedEdgeCount = new AtomicInteger(0);

	private AtomicInteger vertexScheduled = new AtomicInteger(0);
	private AtomicInteger vertexSent = new AtomicInteger(0);
	private AtomicInteger listsReceived = new AtomicInteger(0);

	private AtomicInteger listRequests = new AtomicInteger(0);
	private AtomicInteger requestProcessed = new AtomicInteger(0);

	private volatile int cacheFillRateAdjust = 0;

	private void fillCache() throws IOException
	{
		// Estimate an adequate value for the batch conversion of vertex id.
		int nLocalVertices = adjTable.getLocalData().size();
		int batchSize = nLocalVertices / 50;
		vertexSetTransmitThreshold = Math.min(10 * 1000, Math.max(batchSize, 400));
		vertexSetCapacity = vertexSetTransmitThreshold;
		for (OctojusNode node : getAllocation().getNodes())
		{
			if ( ! node.isLocalNode())
			{
				hostTable.put(node, new LongOpenHashSet(vertexSetCapacity));
			}
		}
		if (useSendThreadPool)
		{
			sendThreadPool = new HashMap<OctojusNode, ExecutorService>();
			for (OctojusNode node : getAllocation().getNodes())
			{
				if ( ! node.isLocalNode())
				{
					sendThreadPool.put(node, Executors.newSingleThreadExecutor());
				}
			}
		}
		StopWatch sw = new StopWatch(UNIT.ms);
		for (LongObjectCursor<LongSet> elt : adjTable.getLocalMap())
		{
			LongSet srcNeighbors = elt.value;
			int srcDegree = srcNeighbors.size();
			if (srcDegree > 1)
			{
				for (LongCursor nelt : srcNeighbors)
				{
					long dst = nelt.value;
					if ( ! ((DHTAllocation) getAllocation()).isLocalElement(dst))
					{
						cacheProcessedEdgeCount.incrementAndGet();
						if (remoteVertexState.get(dst) == NOTINCACHE)
						{
							if (cacheMaxEntries > 0)
							{
								waitForCacheShrink();
							}

							OctojusNode node = ((DHTAllocation) getAllocation())
									.getOwnerNode(dst);
							boolean processVertexSet = false;
							LongOpenHashSet set = null;
							synchronized (hostTable)
							{
								set = hostTable.get(node);
								if (set.add(dst))
								{
									remoteVertexState.put(dst, REQUESTED);

									vertexScheduled.incrementAndGet();
									if (set.size() >= vertexSetTransmitThreshold)
									{
										hostTable.put(node, new LongOpenHashSet(
												vertexSetCapacity));
										processVertexSet = true;
									}
								}
							}
							if (processVertexSet && set != null)
							{
								requestAdjacencyLists(set, node);
							}
						}
						if ((cacheFillRateAdjust > 0)
								&& (cacheProcessedEdgeCount.get() % 10000) == 0)
						{
							try
							{
								Thread.sleep(cacheFillRateAdjust, 0);
							}
							catch (InterruptedException e)
							{
								e.printStackTrace();
							}
						}
					}
				}
			}
		}

		synchronized (hostTable)
		{
			for (Entry<OctojusNode, LongOpenHashSet> entry : hostTable.entrySet())
			{
				OctojusNode node = entry.getKey();
				LongOpenHashSet vertexSet = entry.getValue();
				if (vertexSet != null && vertexSet.size() > 0)
				{
					requestAdjacencyLists(vertexSet, node);
				}
			}
			hostTable.clear();
		}
		long elapsedTimeMs = sw.getElapsedTime();
		if (verbose)
		{
			System.out.println("Cache fill process first step in " + elapsedTimeMs
					+ " ms: " + localEdgeCount.get() + " local edges, "
					+ remoteEdgeCount.get() + " remote edges," + " scheduled "
					+ vertexScheduled.get() + ", sent " + vertexSent.get()
					+ ", received " + listsReceived.get());
		}
		if (useSendThreadPool)
		{
			for (ExecutorService threadPool : sendThreadPool.values())
			{
				threadPool.shutdown();
			}
		}
		try
		{
			StopWatch sw2 = new StopWatch(UNIT.s);
			boolean allTerminated = false;
			while (vertexScheduled.get() > vertexSent.get() || ! allTerminated)
			{
				allTerminated = true;
				if (useSendThreadPool)
				{
					for (ExecutorService threadPool : sendThreadPool.values())
					{
						if ( ! threadPool.awaitTermination(2, TimeUnit.SECONDS))
						{
							allTerminated = false;
							// Stop the loop, because there is no need
							// to check the other thread pools, the
							// condition we are looking for is that all
							// thread pools are terminated.
							break;
						}
					}
				}
				else
				{
					Thread.sleep(1000);
				}
				if (sw2.getElapsedTime() % 120 == 0)
				{
					System.out
							.println("Cache fill process waiting for completion of send: sent "
									+ vertexSent.get()
									+ ", received "
									+ listsReceived.get()
									+ ", cache size "
									+ neighborCache.size()
									+ ", list requests "
									+ listRequests.get()
									+ ", requests processed "
									+ requestProcessed.get());
				}
			}
		}
		catch (InterruptedException ie)
		{
			ie.printStackTrace();
		}

		if (useSendThreadPool)
		{
			sendThreadPool.clear();
		}

		try
		{
			StopWatch sw2 = new StopWatch(UNIT.s);
			while (vertexSent.get() > listsReceived.get())
			{
				Thread.sleep(1000);
				if (sw2.getElapsedTime() % 120 == 0)
				{
					System.out
							.println("Cache fill process waiting for completion of recv: sent "
									+ vertexSent.get()
									+ ", received "
									+ listsReceived.get()
									+ ", cache size "
									+ neighborCache.size()
									+ ", list requests "
									+ listRequests.get()
									+ ", requests processed "
									+ requestProcessed.get());
				}
			}
		}
		catch (InterruptedException ie)
		{
			ie.printStackTrace();
		}

		cacheFilled = true;
		try
		{
			neighborCacheLock.lock();
			cacheEntryAdded.signalAll();
		}
		finally
		{
			neighborCacheLock.unlock();
		}

		long terminatingTimeMs = sw.getElapsedTime();
		if (verbose)
		{
			System.out.println("Cache fill process terminated in " + elapsedTimeMs
					+ " ms + " + terminatingTimeMs + " ms: " + "sent " + vertexSent.get()
					+ ", received " + listsReceived.get() + ", cache size "
					+ neighborCache.size() + " entries.");
		}
	}

	private long previousTime = 0;
	private int previousProcessedEdgeCount = 0;
	private int previousCacheProcessedEdgeCount = 0;

	private void evaluateProgressSpeed(PrintStream logFile, boolean adjust)
	{
		long currentTime = System.nanoTime();
		// Evaluate the speed (in edge/sec.) of the main process
		if (previousTime == 0)
		{
			previousTime = currentTime;
			previousProcessedEdgeCount = processedEdgeCount.get();
			previousCacheProcessedEdgeCount = cacheProcessedEdgeCount.get();
			return;
		}
		long periodUS = (currentTime - previousTime) / 1000;
		int edgeCount = processedEdgeCount.get();
		double processedEdgeSpeed = (edgeCount - previousProcessedEdgeCount)
				/ ((double) periodUS / 1e6);
		previousProcessedEdgeCount = edgeCount;

		// Same for the cache filling process
		edgeCount = cacheProcessedEdgeCount.get();
		double cacheProcessedEdgeSpeed = (edgeCount - previousCacheProcessedEdgeCount)
				/ ((double) periodUS / 1e6);
		previousCacheProcessedEdgeCount = edgeCount;

		// Cache size
		int cacheSize = neighborCache.size();

		//
		int waitingForSend = vertexScheduled.get() - vertexSent.get();
		int waitingForRecv = vertexSent.get() - listsReceived.get();

		if (logFile != null)
		{
			logFile.print((double) currentTime / 1e9);
			logFile.print("  ");
			logFile.print(processedEdgeSpeed);
			logFile.print("  ");
			logFile.print(cacheProcessedEdgeSpeed);
			logFile.print("  ");
			logFile.print(cacheSize);
			logFile.print("  ");
			logFile.print(waitingForSend);
			logFile.print("  ");
			logFile.print(waitingForRecv);
			logFile.println();
		}

		if (adjust)
		{
			double error = (cacheProcessedEdgeSpeed - processedEdgeSpeed);
			int rateCorrection = (int) (1e-4 * error);
			if (rateCorrection < 10)
				rateCorrection = 0;
			else if (rateCorrection > 1000)
				rateCorrection = 1000;
			cacheFillRateAdjust = rateCorrection;
		}
		previousTime = currentTime;
	}

	/**
	 * Wait for the cache to shrink so that its size becomes lower than
	 * {@link #cacheMaxEntries}. Takes a lock on {@link #neighborCacheLock} and
	 * wait on the condition {@link #cacheEntryRemoved} until the size condition
	 * is satisfied.
	 */
	private void waitForCacheShrink()
	{
		try
		{
			neighborCacheLock.lock();
			while (neighborCache.size() > cacheMaxEntries)
			{
				cacheEntryRemoved.await();
			}
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		finally
		{
			neighborCacheLock.unlock();
		}
	}

	@SuppressWarnings("unused")
	private void doRemoteRequests() throws InterruptedException
	{
		try
		{
			LongOpenHashSet vertexSet = null;
			OctojusNode node = null;
			boolean processVertexSet = false;
			synchronized (hostTable)
			{
				hostTable.wait();
				for (Entry<OctojusNode, LongOpenHashSet> entry : hostTable.entrySet())
				{
					node = entry.getKey();
					vertexSet = entry.getValue();
					if (vertexSet.size() > vertexSetTransmitThreshold)
					{
						processVertexSet = true;
						break;
					}
				}
			}
			if (processVertexSet && vertexSet != null && node != null)
			{
				requestAdjacencyLists(vertexSet, node);
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	private LongSet getLocalAdjacencyList(long vertex)
	{
		return getLocalAdjacencyList(vertex, adjTable.get(vertex));
	}

	private LongSet getLocalAdjacencyList(long vertex, LongSet defaultValue)
	{
		return substituteLocalAdjacencyList(vertex, defaultValue);
	}

	private boolean mustBeSubstituted(LongSet set)
	{
		return ( ! (set instanceof HashLongSet32 || set instanceof HashLongSet64) && set
				.size() > 1000);
	}

	private LongSet substituteLocalAdjacencyList(long vertex, LongSet set)
	{
		substitutionLock.readLock().lock();
		if (substitutedNeigbors.containsKey(vertex))
		{
			try
			{
				return substitutedNeigbors.get(vertex);
			}
			finally
			{
				substitutionLock.readLock().unlock();
			}
		}
		else if (mustBeSubstituted(set))
		{
			substitutionLock.readLock().unlock();
			substitutionLock.writeLock().lock();
			try
			{
				LongSet newSet = convertToHashLongSet(set);
				substitutedNeigbors.put(vertex, newSet);
				return newSet;
			}
			finally
			{
				substitutionLock.writeLock().unlock();
			}
		}
		else
		{
			substitutionLock.readLock().unlock();
			return set;
		}
	}

	private LongSet convertToHashLongSet(LongSet set)
	{
		if (set instanceof LongSet32)
		{
			HashLongSet32 newSet = new HashLongSet32(set.size());
			newSet.addAll(set);
			return newSet;
		}
		else if (set instanceof LongSet64)
		{
			HashLongSet64 newSet = new HashLongSet64(set.size());
			newSet.addAll(set);
			return newSet;
		}
		return null;
	}

	private LongSet getAdjacencyList(long vertex)
	{
		if (adjTable.getAllocation().isLocalElement(vertex))
		{
			return getLocalAdjacencyList(vertex);
		}
		else if (cacheFilled)
		{
			return neighborCacheLookup(vertex, false);
		}
		else
		{
			return neighborCacheLookup(vertex, true);
		}
	}

	private LongSet neighborCacheLookup(long vertex, boolean wait)
	{
		neighborCacheLock.lock();
		try
		{
			if (wait)
			{
				if ( ! neighborCache.containsKey(vertex))
				{
					cacheMisses++;
				}
				while ( ! neighborCache.containsKey(vertex))
				{
					cacheEntryAdded.await();
				}
			}
			LongSet result = neighborCache.get(vertex);
			int nCacheAccess;
			synchronized (cacheAccessOps)
			{
				nCacheAccess = cacheAccessOps.get(vertex) + 1;
				cacheAccessOps.put(vertex, nCacheAccess);
			}
			if (nCacheAccess >= vertexOccurrences.get(vertex))
			{
				neighborCache.remove(vertex);
				cacheEntryRemoved.signalAll();
			}
			return result;
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
			return null;
		}
		finally
		{
			neighborCacheLock.unlock();
		}
	}

	private void storeInNeighborCache(long vertex, LongSet adjacencyList)
	{
		neighborCacheLock.lock();
		try
		{
			if (cacheMaxEntries > 0)
			{
				while (neighborCache.size() >= cacheMaxEntries)
				{
					cacheEntryRemoved.await();
				}
			}
			neighborCache.put(vertex, adjacencyList);
			cacheEntryAdded.signalAll();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		finally
		{
			neighborCacheLock.unlock();
		}
	}

	@SuppressWarnings("unused")
	private void requestAdjacencyList(long vertex)
	{
		OctojusNode node = ((DHTAllocation) getAllocation()).getOwnerNode(vertex);
		synchronized (hostTable)
		{
			LongOpenHashSet set = hostTable.get(node);
			if (set.add(vertex))
			{
				vertexScheduled.incrementAndGet();
				remoteVertexState.put(vertex, REQUESTED);
				hostTable.notifyAll();
			}
		}
	}

	/**
	 * 
	 * Will spawn a thread to perform the actual sending of requests and
	 * reception of the adjacency lists if the boolean variable
	 * {@link #useSendThreadPool} is true.
	 * 
	 * @param vertexSet
	 * @param node
	 * @throws IOException
	 */
	private void requestAdjacencyLists(LongOpenHashSet vertexSet, OctojusNode node)
			throws IOException
	{
		if (useSendThreadPool)
		{
			ExecutorService threadPool = sendThreadPool.get(node);
			threadPool.execute(new Runnable()
			{
				@Override
				public void run()
				{
					try
					{
						getRemoteAdjacencyLists(vertexSet, node, true);
					}
					catch (IOException e)
					{
						e.printStackTrace();
					}
				}
			});
		}
		else
		{
			getRemoteAdjacencyLists(vertexSet, node, true);
		}
	}

	/**
	 * Requests a set of adjacency lists for all the vertices in the set
	 * vertexSet. Returns when all the lists have been received. This function
	 * locks the node object.
	 * 
	 * @param vertexSet
	 * @param node
	 * @param storeInCache
	 * @throws IOException
	 */
	private void getRemoteAdjacencyLists(LongOpenHashSet vertexSet, OctojusNode node,
			boolean storeInCache) throws IOException
	{
		TCPConnection connection = connectTo(getRemoteAdjacencyListService, node);
		synchronized (node)
		{
			connection.out.writeInt(vertexSet.size());
			for (LongCursor cr : LongCursor.fromHPPC(vertexSet))
			{
				long vertex = cr.value;
				connection.out.writeLong(vertex);
				vertexSent.incrementAndGet();
			}
			connection.out.flush();
			receiveAdjacencyLists(vertexSet.size(), connection, storeInCache);
		}
	}

	/**
	 * Receives nLists adjacency lists from the connection object. If the
	 * boolean storeInCache is true, then the list is stored in the cache. Calls
	 * {@link #receiveAdjacencyList(TCPConnection, boolean)} to actually receive
	 * each of the nLists lists.
	 * 
	 * @param nLists
	 * @param connection
	 * @param storeInCache
	 * @throws IOException
	 */
	private void receiveAdjacencyLists(int nLists, TCPConnection connection,
			boolean storeInCache) throws IOException
	{
		for (int i = 0; i < nLists; i++)
		{
			receiveAdjacencyList(connection, storeInCache);
		}
	}

	/**
	 * Receives one adjacency list from the connection object. Returns when the
	 * list is completely received. If the boolean storeInCache is true, the
	 * list is stored in the cache.
	 * 
	 * This function locks the cache when storing the list in it.
	 * 
	 * @param connection
	 * @param storeInCache
	 * @return
	 * @throws IOException
	 */
	private LongSet receiveAdjacencyList(TCPConnection connection, boolean storeInCache)
			throws IOException
	{
		long vertex = connection.in.readLong();
		int listSize = connection.in.readInt();
		int eltSize = connection.in.readByte();
		LongSet list = null;

		if (eltSize == 32)
		{
			list = new HashLongSet32(listSize);
			for (int i = 0; i < listSize; i++)
			{
				int neighbor = connection.in.readInt();
				list.add(neighbor);
			}
		}
		else if (eltSize == 64)
		{
			list = new HashLongSet64(listSize);
			for (int i = 0; i < listSize; i++)
			{
				long neighbor = connection.in.readLong();
				list.add(neighbor);
			}
		}
		listsReceived.incrementAndGet();
		if (storeInCache)
		{
			storeInNeighborCache(vertex, list);
			remoteVertexState.put(vertex, INCACHE);
		}
		return list;
	}

	/**
	 * Request and returns the adjacency list of a remote vertex. This function
	 * is completely synchronous and returns when the whole list has been
	 * completely received.
	 * 
	 * @param vertex
	 * @param storeInCache
	 * @return
	 */
	@SuppressWarnings("unused")
	private LongSet getRemoteAdjacencyList(long vertex, boolean storeInCache)
	{
		OctojusNode node = adjTable.getAllocation().getOwnerNode(vertex);
		synchronized (node)
		{
			TCPConnection connection = connectTo(getRemoteAdjacencyListService, node);
			try
			{
				connection.out.writeInt(1); // only one vertex to process
				connection.out.writeLong(vertex);
				connection.out.flush();
				vertexSent.incrementAndGet();
				return receiveAdjacencyList(connection, storeInCache);
			}
			catch (IOException e)
			{
				e.printStackTrace();
				return null;
			}
		}
	}

	private Service getRemoteAdjacencyListService = new Service()
	{
		@Override
		public void serveThrough(FullDuplexDataConnection2 connection)
				throws ClassNotFoundException, IOException
		{
			int nVertices = connection.in.readInt();
			long[] vertices = new long[nVertices];
			for (int i = 0; i < nVertices; i++)
			{
				vertices[i] = connection.in.readLong();
				listRequests.incrementAndGet();
			}
			for (int i = 0; i < nVertices; i++)
			{
				long vertex = vertices[i];
				LongSet adjList = adjTable.get(vertex);
				connection.out.writeLong(vertex);
				connection.out.writeInt(adjList.size());
				if (adjList instanceof LongSet32)
				{
					connection.out.writeByte(32);
					for (LongCursor cr : adjList)
					{
						connection.out.writeInt((int) cr.value);
					}
				}
				else
				{
					connection.out.writeByte(64);
					for (LongCursor cr : adjList)
					{
						connection.out.writeLong(cr.value);
					}
				}
				connection.out.flush();
				requestProcessed.incrementAndGet();
			}
		}
	};

	@Override
	public void clearLocalData()
	{
		hostTable = null;
		sendThreadPool = null;
		neighborCache = null;
		cacheAccessOps = null;
		vertexOccurrences = null;
		substitutedNeigbors = null;
	}
}