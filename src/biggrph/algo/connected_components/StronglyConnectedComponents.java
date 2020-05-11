package biggrph.algo.connected_components;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import ldjo.bsp.ArrayListBox;
import ldjo.bsp.BSPActivityPrinter;
import ldjo.bsp.BSPComputation;
import ldjo.bsp.Box;
import ldjo.bsp.SingleMessageBox;
import ldjo.bsp.BSPThreadSpecifics;
import ldjo.bsp.msg.LongMessage;
import octojus.ComputationRequest;
import octojus.NoReturn;
import octojus.OctojusNode;
import octojus.OneNodeOneRequest;
import toools.StopWatch;
import toools.StopWatch.UNIT;
import toools.collection.bigstuff.longset.LongCursor;
import toools.io.FullDuplexDataConnection2;
import toools.net.TCPConnection;
import toools.thread.MultiThreadPolicy;
import toools.thread.NThreadsPolicy;
import biggrph.BigAdjacencyTable;
import biggrph.GraphTopology;
import biggrph.algo.connected_components.StronglyConnectedComponents.TarjanSCC.LocalSCCResults;
import bigobject.Allocation;
import bigobject.BigObjectComputingRequest;
import bigobject.BigObjectRegistry;
import bigobject.LiveDistributedObject;
import bigobject.Service;

import com.carrotsearch.hppc.IntLongOpenHashMap;
import com.carrotsearch.hppc.LongByteOpenHashMap;
import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.carrotsearch.hppc.LongLongOpenHashMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap.KeysContainer;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import com.carrotsearch.hppc.cursors.LongObjectCursor;

import dht.ByteDHT;
import dht.DHTAllocation;
import dht.LongDHT;
import dht.set.HashLongSet64;
import dht.set.LongSet;

public class StronglyConnectedComponents
{
	private final String SCC_FINALIDS_SUFFIX = "/sccFinalIds";
	private final String SCC_FINALMARK_SUFFIX = "/sccIsFinal";

	private GraphTopology inputGraph;
	private DHTAllocation allocation;

	private boolean verbose = false;
	private final boolean useSingleVertexComponentsBSP = true;
	private boolean useLocalSCCExecutions = true;
	static private final boolean useIterativeLocalSCC = true;
	private boolean deleteInputGraph = false;

	@SuppressWarnings("unused")
	private int nThreads = 1;
	private boolean saveResults = false;
	private boolean skipSingleVertexSCCDetection = false;

	public StronglyConnectedComponents(GraphTopology inputGraph)
	{
		this.inputGraph = inputGraph;
		this.allocation = (DHTAllocation) inputGraph.getAllocation();
	}

	public void setVerbose(boolean verbose)
	{
		this.verbose = verbose;
	}

	public void setNThreads(int nThreads)
	{
		this.nThreads = nThreads;
	}

	public void useLocalSCCExecutions(boolean useLocalSCC)
	{
		this.useLocalSCCExecutions = useLocalSCC;
	}

	public void setSaveResults(boolean saveResults)
	{
		this.saveResults = saveResults;
	}

	public void skipSingleVertexSCCDetection(boolean skip)
	{
		this.skipSingleVertexSCCDetection = skip;
	}

	public void deleteInputGraph(boolean delInputGraph)
	{
		this.deleteInputGraph = delInputGraph;
	}

	public LongDHT execute()
	{
		GraphTopology workGraph = inputGraph;
		LongDHT sccLocalSccIds = null;

		if (useLocalSCCExecutions)
		{
			long nVertices = workGraph.getOutAdjacencyTable().getNumberOfVertices();
			sccLocalSccIds = new LongDHT(workGraph.getID() + "/localSCCIds",
					workGraph.getAllocation(), nVertices);

			if (useIterativeLocalSCC)
			{
				IterativeSCC itscc = new IterativeSCC(workGraph, sccLocalSccIds);
				Map<OctojusNode, LocalSCCResults> infos = itscc
						.execute(workGraph.getAllocation().getNodes());
				if (verbose)
				{
					for (Entry<OctojusNode, LocalSCCResults> entry : infos.entrySet())
					{
						System.out.println(entry.getKey() + ": " + entry.getValue());
					}
				}
				itscc.delete();
			}
			else
			{
				TarjanSCC tscc = new TarjanSCC(workGraph, sccLocalSccIds);
				Map<OctojusNode, LocalSCCResults> infos = tscc
						.execute(workGraph.getAllocation().getNodes());
				if (verbose)
				{
					for (Entry<OctojusNode, LocalSCCResults> entry : infos.entrySet())
					{
						System.out.println(entry.getKey() + ": " + entry.getValue());
					}
				}
				tscc.delete();
			}
			if (allocation.getNodes().size() == 1)
			{
				// No need to run a parallel implementation of SCC detection:
				// all
				// the results are already in the locally detected SCCs.
				return sccLocalSccIds;
			}
			// Build the reduced graph using sccLocalSccIds to map vertices from
			// the original graph to the new one.
			GraphTopology reducedGraph = new GraphTopology(workGraph.getID() + "/reduced",
					workGraph.getAllocation(), workGraph.isBiDirectional(),
					workGraph.isMultigraph(), 0);
			GraphConverter converter = new GraphConverter(workGraph, sccLocalSccIds,
					reducedGraph, verbose);
			converter.execute();
			converter.delete();
			if (verbose)
			{
				System.out
						.println("Reduced Graph: " + reducedGraph.getOutAdjacencyTable());
				System.out.println("  vertices: " + reducedGraph.getNumberOfVertices());
				System.out.println("  edges: " + reducedGraph.getNumberOfEdges());
				System.out.println("  avg out degree: "
						+ reducedGraph.getOutAdjacencyTable().getAverageDegree());
				System.out.println("  max out degree: "
						+ reducedGraph.getOutAdjacencyTable().getMaxDegree());
			}
			if (saveResults)
			{
				if (verbose)
				{
					System.out.println("Saving reduced graph " + reducedGraph.toString());
				}
				reducedGraph.save();
			}
			workGraph = reducedGraph;
			if (deleteInputGraph)
			{
				inputGraph.delete();
				inputGraph = null;
			}
		}
		if ( ! workGraph.isBiDirectional())
		{
			workGraph.setBiDirectional(true, verbose);
		}
		long nVertices = workGraph.getOutAdjacencyTable().getNumberOfVertices();
		LongDHT sccIds = new LongDHT(workGraph.getID() + SCC_FINALIDS_SUFFIX, allocation,
				nVertices);
		/*
		 * Use a ByteDHT to store the boolean information that a vertex has
		 * received
		 * its definitive SCC id. ByteDHT.get() will return 0 if no value has
		 * been
		 * set for a given vertex. So we use this 0 value as the boolean false,
		 * and
		 * 1 for true.
		 */
		ByteDHT sccIsFinal = new ByteDHT(workGraph.getID() + SCC_FINALMARK_SUFFIX,
				allocation, nVertices);

		initialize(workGraph, sccIds, sccIsFinal);
		if ( ! skipSingleVertexSCCDetection)
		{
			trimSingleVertexComponents(workGraph, sccIds, sccIsFinal);
		}
		while (countUnassignedVertices(workGraph, sccIsFinal) > 0)
		{
			forwardSCCPass(workGraph, sccIds, sccIsFinal);
			backwardSCCPass(workGraph, sccIds, sccIsFinal);
		}
		// Final cleanup.
		sccIsFinal.delete();
		sccIsFinal = null;

		if (useLocalSCCExecutions)
		{
			// compose the two mappings vertex -> sccId in sccLocalSccIds
			// and sccIds to build the final results.
			LongDHTComposer composer = new LongDHTComposer(sccLocalSccIds, sccIds,
					sccLocalSccIds);
			composer.delete();
			// cleanup, the workGraph is actually the reduced one, no need
			// to keep it.
			workGraph.delete();
			if (saveResults)
			{
				sccLocalSccIds.save();
			}
			return sccLocalSccIds;
		}
		else
		{
			if (saveResults)
			{
				sccIds.save();
			}
			return sccIds;
		}
	}

	@SuppressWarnings("hiding")
	private void initialize(final GraphTopology inputGraph, LongDHT sccIds,
			ByteDHT sccIsFinal)
	{
		new OneNodeOneRequest<NoReturn>()
		{
			@Override
			protected ComputationRequest<NoReturn> createComputationRequestForNode(
					OctojusNode n)
			{
				InitSCCRequest job = new InitSCCRequest();
				job.topologyID = inputGraph.getID();
				job.sccID = sccIds.getID();
				job.sccValidID = sccIsFinal.getID();
				return job;
			}
		}.execute(allocation.getNodes());
	}

	@SuppressWarnings("serial")
	static class InitSCCRequest extends ComputationRequest<NoReturn>
	{
		String topologyID;
		String sccID;
		String sccValidID;

		@Override
		protected NoReturn compute() throws Throwable
		{
			GraphTopology topology = (GraphTopology) BigObjectRegistry.defaultRegistry
					.get(topologyID);
			LongDHT sccIds = (LongDHT) BigObjectRegistry.defaultRegistry.get(sccID);
			ByteDHT sccIsFinal = (ByteDHT) BigObjectRegistry.defaultRegistry
					.get(sccValidID);
			for (LongObjectCursor<LongSet> current : topology.getOutAdjacencyTable()
					.getLocalData())
			{
				long vertexId = current.key;
				sccIds.set(vertexId, vertexId);
				sccIsFinal.set(vertexId, (byte) 0);
			}
			return null;
		}
	}

	@SuppressWarnings({ "unchecked", "hiding" })
	private void trimSingleVertexComponents(GraphTopology inputGraph, LongDHT sccFinalIds,
			ByteDHT sccIsFinal)
	{
		if (useSingleVertexComponentsBSP)
		{
			SingleVertexComponentsBSP bsp = new SingleVertexComponentsBSP(
					inputGraph.getID() + "/TrimBSP", allocation, inputGraph.getID(),
					sccFinalIds.getID(), sccIsFinal.getID(), verbose);
			if (verbose)
				bsp.addListener(BSPActivityPrinter.DEFAULT_INSTANCE);
			else
				bsp.addListener(bspPrinter);
			bsp.execute();
			bsp.delete();
		}
		else
		{
			SingleVertexComponentsTrim computation = new SingleVertexComponentsTrim(
					inputGraph, sccFinalIds, sccIsFinal);
			computation.setVerbose(verbose);
			computation.execute();
			computation.delete();
		}
	}

	static class SingleVertexComponentsBSP
			extends BSPComputation<GraphTopology, LongMessage, Serializable>
	{
		private LongIntOpenHashMap outDegrees;
		private LongIntOpenHashMap inDegrees;

		private LongDHT sccFinalIds;
		private ByteDHT sccIsFinal;
		private Object sccIdsLock = new Object();
		private AtomicInteger sccCount = new AtomicInteger(0);
		private AtomicInteger sccFoundInStep = new AtomicInteger(0);

		private boolean verbose = false;

		@SuppressWarnings("unchecked")
		public SingleVertexComponentsBSP(String id, DHTAllocation allocation,
				String graphID, String sccID, String sccValidID, boolean verbose)
		{
			this(id, allocation, graphID, LongMessage.class,
					(Class<? extends Box<LongMessage>>) ArrayListBox.class, false,
					new NThreadsPolicy(1), sccID, sccValidID, verbose);
		}

		@SuppressWarnings("unchecked")
		public SingleVertexComponentsBSP(String id, DHTAllocation allocation,
				String graphID, String sccID, String sccValidID, int nThreads,
				boolean verbose)
		{
			this(id, allocation, graphID, LongMessage.class,
					(Class<? extends Box<LongMessage>>) ArrayListBox.class, false,
					new NThreadsPolicy(nThreads), sccID, sccValidID, verbose);
		}

		protected SingleVertexComponentsBSP(String id, DHTAllocation allocation,
				String graphID, Class<LongMessage> messageClass,
				Class<? extends Box<LongMessage>> boxClass, boolean asynchronous,
				MultiThreadPolicy policy, String sccID, String sccValidID,
				Boolean verbose)
		{
			this(id, allocation, graphID, messageClass, boxClass, asynchronous, policy,
					defaultMessagePacketSize, sccID, sccValidID, verbose);
		}

		protected SingleVertexComponentsBSP(String id, DHTAllocation allocation,
				String graphID, Class<LongMessage> messageClass,
				Class<? extends Box<LongMessage>> boxClass, boolean asynchronous,
				MultiThreadPolicy policy, int messagePacketSize, String sccID, String sccValidID,
				Boolean verbose)		
		{
			super(id, allocation, graphID, messageClass, boxClass, asynchronous, policy, messagePacketSize,
					sccID, sccValidID, verbose);
			int nLocalVertices = getTarget().getNumberOfLocalVertices();
			outDegrees = new LongIntOpenHashMap(nLocalVertices);
			inDegrees = new LongIntOpenHashMap(nLocalVertices);
			initializeOutDegrees();
			initializeInDegrees();
			sccFinalIds = (LongDHT) BigObjectRegistry.defaultRegistry.get(sccID);
			sccIsFinal = (ByteDHT) BigObjectRegistry.defaultRegistry.get(sccValidID);
			this.verbose = verbose;
			setScheduleAllVertices(true);
		}

		private void initializeOutDegrees()
		{
			BigAdjacencyTable outTable = getTarget().getOutAdjacencyTable();
			Iterator<LongObjectCursor<LongSet>> iterator = outTable.getLocalData()
					.iterator();
			while (iterator.hasNext())
			{
				LongObjectCursor<LongSet> current = iterator.next();
				long vertexId = current.key;
				int degree = (current.value != null ? current.value.size() : 0);
				outDegrees.put(vertexId, degree);
			}
		}

		private void initializeInDegrees()
		{
			BigAdjacencyTable inTable = getTarget().getInAdjacencyTable();
			Iterator<LongObjectCursor<LongSet>> iterator = inTable.getLocalData()
					.iterator();
			while (iterator.hasNext())
			{
				LongObjectCursor<LongSet> current = iterator.next();
				long vertexId = current.key;
				int degree = (current.value != null ? current.value.size() : 0);
				inDegrees.put(vertexId, degree);
			}
		}

		@Override
		protected void stepStartHook(int superStep)
		{
			sccFoundInStep.set(0);
		}

		@Override
		protected void stepEndHook(int superStep)
		{
			if (superStep == 0)
				setScheduleAllVertices(false);
			if (verbose)
			{
				System.out.println(
						"Found " + sccFoundInStep.get() + " SCCs in step " + superStep);
				System.out
						.println("Step " + superStep + ": " + sccCount.get() + " SCCs.");
			}
		}

		@Override
		protected void computeLocalElement(GraphTopology gTopology, long superstep,
				long vertex, Box<LongMessage> inbox,
				BSPThreadSpecifics<GraphTopology, LongMessage> threadInfo)
		{
			if (sccIsFinal.get(vertex) != 0)
			{
				return;
			}
			if (inbox != null && inbox.size() > 0)
			{
				LongSet outNeighbors = gTopology.getOutNeighbors(vertex);
				LongSet inNeighbors = gTopology.getInNeighbors(vertex);
				for (LongMessage message : inbox)
				{
					if (outNeighbors != null && outNeighbors.contains(message.value))
					{
						synchronized (outDegrees)
						{
							int currentDegree = outDegrees.get(vertex);
							if (currentDegree > 0)
								outDegrees.put(vertex, currentDegree - 1);
						}
					}
					if (inNeighbors != null && inNeighbors.contains(message.value))
					{
						synchronized (inDegrees)
						{
							int currentDegree = inDegrees.get(vertex);
							if (currentDegree > 0)
								inDegrees.put(vertex, currentDegree - 1);
						}
					}
				}
			}
			if (inDegrees.get(vertex) == 0 || outDegrees.get(vertex) == 0)
			{
				synchronized (sccIdsLock)
				{
					sccFinalIds.set(vertex, vertex);
					sccIsFinal.set(vertex, (byte) 1);
				}
				sccCount.incrementAndGet();
				@SuppressWarnings("unused")
				int nScc = sccFoundInStep.incrementAndGet();
				LongMessage message = new LongMessage(vertex);
				threadInfo.post(message, gTopology.getOutNeighbors(vertex));
				threadInfo.post(message, gTopology.getInNeighbors(vertex));
			}
		}

		@Override
		protected Serializable getLocalResult()
		{
			return null;
		}

		@Override
		public void clearLocalData()
		{
			super.clearLocalData();
			// outDegrees.clear();
			// inDegrees.clear();
			outDegrees = null;
			inDegrees = null;
		}
	}

	@SuppressWarnings({ "unchecked", "hiding" })
	private void forwardSCCPass(final GraphTopology inputGraph, final LongDHT sccFinalIds,
			final ByteDHT sccIsFinal)
	{
		ForwardSCC bsp = new ForwardSCC(inputGraph.getID() + "/ForwardSCC", allocation,
				inputGraph.getID(), sccFinalIds.getID(), sccIsFinal.getID());
		if (verbose)
			bsp.addListener(BSPActivityPrinter.DEFAULT_INSTANCE);
		else
			bsp.addListener(bspPrinter);
		bsp.execute();
		bsp.delete();
	}

	static class ForwardSCC
			extends BSPComputation<GraphTopology, LongMessage, Serializable>
	{
		private LongDHT sccFinalIds;
		private ByteDHT sccIsFinal;

		@SuppressWarnings("unchecked")
		public ForwardSCC(String id, DHTAllocation allocation, String graphID,
				String sccID, String sccValidID)
		{
			this(id, allocation, graphID, LongMessage.class,
					(Class<? extends Box<LongMessage>>) SingleMessageBox.class, false,
					new NThreadsPolicy(1), sccID, sccValidID);
		}

		ForwardSCC(String id, DHTAllocation allocation, String graphID,
				Class<LongMessage> messageClass,
				Class<? extends Box<LongMessage>> boxClass, boolean asynchronous,
				MultiThreadPolicy threadPolicy, String sccID, String sccValidID)
		{
			this(id, allocation, graphID, messageClass, boxClass, asynchronous,
					threadPolicy, defaultMessagePacketSize, sccID, sccValidID);
		}

		@SuppressWarnings("unchecked")
		ForwardSCC(String id, DHTAllocation allocation, String graphID,
				Class<LongMessage> messageClass,
				Class<? extends Box<LongMessage>> boxClass, boolean asynchronous,
				MultiThreadPolicy threadPolicy, int messagePacketSize, String sccID, String sccValidID)
		{
			super(id, allocation, graphID, LongMessage.class,
					(Class<? extends Box<LongMessage>>) SingleMessageBox.class, false,
					new NThreadsPolicy(1), messagePacketSize, sccID, sccValidID);
			sccFinalIds = (LongDHT) BigObjectRegistry.defaultRegistry.get(sccID);
			sccIsFinal = (ByteDHT) BigObjectRegistry.defaultRegistry.get(sccValidID);
			setScheduleAllVertices(true);
		}

		@Override
		protected void stepStartHook(int superStep)
		{
//			if (superStep == 0)
//				setMessagePacketSize( 1000 );
		}

		@Override
		protected void stepEndHook(int superStep)
		{
			if (superStep == 0)
				setScheduleAllVertices(false);
		}

		@Override
		protected void computeLocalElement(GraphTopology gTopology, long superstep,
				long vertex, Box<LongMessage> inbox,
				BSPThreadSpecifics<GraphTopology, LongMessage> threadInfo)
		{
			// Do not process vertices that are already assigned to a SCC.
			if (sccIsFinal.get(vertex) != 0)
			{
				return;
			}
			boolean changed = false;
			long currentScc;
			// At first super step, all vertices are labeled with their vertex
			// number as the SCC id.
			if (superstep == 0)
			{
				currentScc = vertex;
			}
			// At all other super steps, use either the current label of the
			// vertex recorded in the sccFinalIds map, or the vertex id if no
			// information has been recorded yet.
			// LongDHT.get() returns 0 if the map does not contain a value for
			// the key, but 0 is a valid vertex id. We need to distinguish the
			// two cases by using the containsKey() function.
			else if (sccFinalIds.containsKey(vertex))
			{
				currentScc = sccFinalIds.get(vertex);
			}
			else
			{
				currentScc = vertex;
			}
			for (LongMessage msg : inbox)
			{
				if (msg.value < currentScc)
				{
					currentScc = msg.value;
					changed = true;
				}
			}
			if (changed || superstep == 0)
			{
				sccFinalIds.set(vertex, currentScc);
				LongMessage msg = new LongMessage(currentScc);
				threadInfo.post(msg, gTopology.getOutNeighbors(vertex));
			}
		}

		@Override
		protected void combine(Box<LongMessage> box, LongMessage newMessage)
		{
			synchronized (box)
			{
				if (box.isEmpty())
				{
					box.add(newMessage);
				}
				else
				{
					Iterator<LongMessage> itr = box.iterator();
					boolean add = false;
					while (itr.hasNext())
					{
						LongMessage msg = itr.next();
						if (newMessage.value < msg.value)
						{
							add = true;
							itr.remove();
						}
					}
					if (add)
						box.add(newMessage);
						
//					LongMessage msg = box.get(0);
//					if (newMessage.value < msg.value)
//					{
//						box.clear();
//						box.add(newMessage);
//					}
				}
			}
		}

		@Override
		protected Serializable getLocalResult()
		{
			return null;
		}
	}

	@SuppressWarnings({ "unchecked", "hiding" })
	private void backwardSCCPass(final GraphTopology inputGraph,
			final LongDHT sccFinalIds, final ByteDHT sccIsFinal)
	{
		BackwardSCC bsp = new BackwardSCC(inputGraph.getID() + "/BackwardSCC", allocation,
				inputGraph.getID(), sccFinalIds.getID(), sccIsFinal.getID());
		if (verbose)
			bsp.addListener(BSPActivityPrinter.DEFAULT_INSTANCE);
		else
			bsp.addListener(bspPrinter);
		bsp.execute();
		bsp.delete();
	}

	static class BackwardSCC
			extends BSPComputation<GraphTopology, LongMessage, Serializable>
	{
		private LongDHT sccFinalIds;
		private ByteDHT sccIsFinal;

		@SuppressWarnings("unchecked")
		public BackwardSCC(String id, DHTAllocation allocation, String graphID,
				String sccID, String sccValidID)
		{
			this(id, allocation, graphID, LongMessage.class,
					(Class<? extends Box<LongMessage>>) ArrayListBox.class, false,
					new NThreadsPolicy(1), sccID, sccValidID);
		}

		BackwardSCC(String id, DHTAllocation allocation, String graphID,
				Class<LongMessage> messageClass,
				Class<? extends Box<LongMessage>> boxClass, boolean asynchronous,
				MultiThreadPolicy threadPolicy, String sccID, String sccValidID)
		{
			this(id, allocation, graphID, messageClass, boxClass, asynchronous,
					threadPolicy, defaultMessagePacketSize, sccID, sccValidID);
		}

		BackwardSCC(String id, DHTAllocation allocation, String graphID,
				Class<LongMessage> messageClass,
				Class<? extends Box<LongMessage>> boxClass, boolean asynchronous,
				MultiThreadPolicy threadPolicy, int messagePacketSize, String sccID, String sccValidID)
		{
			super(id, allocation, graphID, messageClass, boxClass, asynchronous,
					threadPolicy, messagePacketSize, sccID, sccValidID);
			sccFinalIds = (LongDHT) BigObjectRegistry.defaultRegistry.get(sccID);
			sccIsFinal = (ByteDHT) BigObjectRegistry.defaultRegistry.get(sccValidID);
			setScheduleAllVertices(true);
		}

		@Override
		protected void stepEndHook(int superStep)
		{
			if (superStep == 0)
				setScheduleAllVertices(false);
		}

		@Override
		protected void computeLocalElement(GraphTopology graph, long superstep,
				long vertex, Box<LongMessage> inbox,
				BSPThreadSpecifics<GraphTopology, LongMessage> threadInfo)
		{
			boolean hasSCC = sccFinalIds.containsKey(vertex);
			boolean isFinalSCC = (sccIsFinal.get(vertex) != 0);
			long sccId = sccFinalIds.get(vertex);

			if (superstep == 0)
			{
				// Proceed from the roots of the SCC as found in the previous
				// ForwardSCC pass. Roots
				// have a SCC id equal to their vertex id and we are interested
				// only in candidate
				// SCC roots, those that are not yet validated.
				if (hasSCC && ! isFinalSCC && sccId == vertex)
				{
					sccIsFinal.set(vertex, (byte) 1);
					threadInfo.post(new LongMessage(sccId), graph.getInNeighbors(vertex));
				}
			}
			else
			{
				if (hasSCC && ! isFinalSCC)
				{
					for (LongMessage msg : inbox)
					{
						if (msg.value == sccId)
						{
							sccIsFinal.set(vertex, (byte) 1);
							threadInfo.post(msg, graph.getInNeighbors(vertex));
							break;
						}
					}
				}
			}
		}

		@Override
		protected void combine(Box<LongMessage> box, LongMessage newMessage)
		{
			synchronized (box)
			{
				boolean keepNewMessage = true;
				for (LongMessage msg : box)
				{
					if (msg.value == newMessage.value)
					{
						keepNewMessage = false;
						break;
					}
				}
				if (keepNewMessage)
				{
					box.add(newMessage);
				}
			}
		}

		@Override
		protected Serializable getLocalResult()
		{
			return null;
		}
	}

	private long countUnassignedVertices(
			@SuppressWarnings("hiding") final GraphTopology inputGraph,
			ByteDHT sccIsFinal)
	{
		Map<OctojusNode, Long> counts = new OneNodeOneRequest<Long>()
		{
			@Override
			protected ComputationRequest<Long> createComputationRequestForNode(
					OctojusNode n)
			{
				return new UnassignedVertexCountRequest(inputGraph.getID(),
						sccIsFinal.getID());
			}
		}.execute(inputGraph.getAllocation().getNodes());
		long result = 0;
		for (long value : counts.values())
		{
			result += value;
		}
		return result;
	}

	@SuppressWarnings("serial")
	static private class UnassignedVertexCountRequest
			extends BigObjectComputingRequest<GraphTopology, Long>
	{
		private String sccValidID;

		public UnassignedVertexCountRequest(String bigObjectID, String sccValidID)
		{
			super(bigObjectID);
			this.sccValidID = sccValidID;
		}

		@Override
		protected Long localComputation(GraphTopology graph) throws Throwable
		{
			ByteDHT sccIsFinal = (ByteDHT) BigObjectRegistry.defaultRegistry
					.get(sccValidID);
			long count = 0;
			for (LongObjectCursor<LongSet> elt : graph.getLocalMap())
			{
				long vertex = elt.key;
				if (sccIsFinal.get(vertex) == 0)
				{
					count += 1;
				}
			}
			return count;
		}
	}

	private BSPActivityPrinter bspPrinter = new BSPActivityPrinter()
	{
		private int runs = 0;

		@SuppressWarnings("rawtypes")
		@Override
		public void computationStart(BSPComputation bsp)
		{
			System.out.println("Running SCC");
			runs = 0;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public void stepStarting(BSPComputation bsp, int superstep)
		{
		}

		@SuppressWarnings("rawtypes")
		@Override
		public void stepDone(BSPComputation bsp, List superstepInfos)
		{
			System.out.print('.');
			System.out.flush();
			runs++;
			if ((runs % 50) == 0)
				System.out.println();
		}

		@SuppressWarnings("rawtypes")
		@Override
		public void computationEnd(BSPComputation bsp)
		{
			System.out.println();
			System.out.println("SCC completed in " + runs + " steps.");
		}
	};

	private static class SingleVertexComponentsTrim
			extends LiveDistributedObject<Serializable, Serializable>
	{
		private LongIntOpenHashMap outDegrees;
		private LongIntOpenHashMap inDegrees;

		private GraphTopology gTopology;
		DHTAllocation allocation;
		private LongDHT sccFinalIds;
		private ByteDHT sccIsFinal;

		LongOpenHashSet queue;
		LongOpenHashSet queueNext;
		Object queueLock = new Object();

		private int vertexProcessed = 0;
		private int sccAssigned = 0;
		private int remoteSend = 0;
		private int remoteReceived = 0;
		private int remoteQueued = 0;
		private int remoteIgnored = 0;

		private boolean verbose = false;

		public SingleVertexComponentsTrim(GraphTopology graph, LongDHT sccFinalsIds,
				ByteDHT sccIsFinal)
		{
			this(graph.getID() + "/TrimPhase", graph.getAllocation(), graph.getID(),
					sccFinalsIds.getID(), sccIsFinal.getID());
		}

		protected SingleVertexComponentsTrim(String id, DHTAllocation allocation,
				String graphID, String sccID, String sccValidID)
		{
			super(id, allocation, graphID, sccID, sccValidID);
			gTopology = (GraphTopology) BigObjectRegistry.defaultRegistry.get(graphID);
			this.allocation = allocation;
			sccFinalIds = (LongDHT) BigObjectRegistry.defaultRegistry.get(sccID);
			sccIsFinal = (ByteDHT) BigObjectRegistry.defaultRegistry.get(sccValidID);

			int nLocalVertices = gTopology.getNumberOfLocalVertices();
			outDegrees = new LongIntOpenHashMap(nLocalVertices);
			inDegrees = new LongIntOpenHashMap(nLocalVertices);
			initializeOutDegrees();
			initializeInDegrees();
			@SuppressWarnings("rawtypes")
			KeysContainer localVertices = gTopology.getOutAdjacencyTable().getLocalMap()
					.keys();
			queue = new LongOpenHashSet(localVertices);
			queueNext = new LongOpenHashSet(gTopology.getNumberOfLocalVertices());
		}

		protected SingleVertexComponentsTrim(String id, Allocation a,
				Object... extraParameters)
		{
			super(id, a, extraParameters);
		}

		public void setVerbose(boolean verbose)
		{
			this.verbose = verbose;
		}

		@Override
		public void clearLocalData()
		{
		}

		private void initializeOutDegrees()
		{
			BigAdjacencyTable outTable = gTopology.getOutAdjacencyTable();
			Iterator<LongObjectCursor<LongSet>> iterator = outTable.getLocalData()
					.iterator();
			while (iterator.hasNext())
			{
				LongObjectCursor<LongSet> current = iterator.next();
				long vertexId = current.key;
				int degree = (current.value != null ? current.value.size() : 0);
				outDegrees.put(vertexId, degree);
			}
		}

		private void initializeInDegrees()
		{
			BigAdjacencyTable inTable = gTopology.getInAdjacencyTable();
			Iterator<LongObjectCursor<LongSet>> iterator = inTable.getLocalData()
					.iterator();
			while (iterator.hasNext())
			{
				LongObjectCursor<LongSet> current = iterator.next();
				long vertexId = current.key;
				int degree = (current.value != null ? current.value.size() : 0);
				inDegrees.put(vertexId, degree);
			}
		}

		private void resetCounters()
		{
			vertexProcessed = 0;
			// sccAssigned = 0;
			// remoteSend = 0;
			// remoteReceived = 0;
		}

		public void execute()
		{
			int iterCount = 0;
			TrimPhaseInfos globalInfos;
			do
			{
				Map<OctojusNode, TrimPhaseInfos> executionInfos = new OneNodeOneRequest<TrimPhaseInfos>()
				{
					@Override
					protected ComputationRequest<TrimPhaseInfos> createComputationRequestForNode(
							OctojusNode n)
					{
						return new SingletonSCCExecutionRequest(getID(), verbose);
					}
				}.execute(allocation.getNodes());
				globalInfos = new TrimPhaseInfos(0, 0, 0, 0, 0, 0, 0);
				for (OctojusNode node : allocation.getNodes())
				{
					// System.out.println(executionInfos.get(node));
					globalInfos = globalInfos.add(executionInfos.get(node));
				}
				iterCount++;
				if (verbose)
				{
					System.out.println("Iterationg " + iterCount + ": " + globalInfos);
				}
				else
				{
					System.out.print('.');
					System.out.flush();
					if ((iterCount % 50) == 0)
						System.out.println();
				}
			}
			while ((globalInfos.vertexProcessed > 0 || globalInfos.remoteSend > 0
					|| globalInfos.nextQueueSize > 0)
					&& globalInfos.remoteQueued
							+ globalInfos.remoteIgnored < globalInfos.remoteSend);
		}

		TrimPhaseInfos localComputation(@SuppressWarnings("hiding") boolean verbose)
		{
			resetCounters();
			// System.out.println("Queue on entry: " + queue);
			// System.out.println("QueueNext on entry: " + queueNext);
			if (queue.isEmpty() && ! queueNext.isEmpty())
			{
				exchangeQueues();
			}
			while ( ! queue.isEmpty())
			{
				// System.out.println("Queue: " + queue);
				// System.out.println("QueueNext: " + queueNext);
				for (LongCursor cursor : LongCursor.fromHPPC(queue))
				{
					long vertex = cursor.value;
					// System.out.println("Processing " + vertex);
					boolean inSCC = (sccIsFinal.get(vertex) != 0);
					if ( ! inSCC)
					{
						int degreeIn;
						int degreeOut;
						synchronized (inDegrees)
						{
							degreeIn = inDegrees.get(vertex);
						}
						synchronized (outDegrees)
						{
							degreeOut = outDegrees.get(vertex);
						}
						if (degreeIn == 0 || degreeOut == 0)
						{
							sccFinalIds.set(vertex, vertex);
							sccIsFinal.set(vertex, (byte) 1);
							sccAssigned++;
							// System.out.println("Assigning " + vertex);
							for (LongCursor nCursor : gTopology.getOutNeighbors(vertex))
							{
								long neighbor = nCursor.value;
								if (allocation.isLocalElement(neighbor))
								{
									// If the neighbor is already in a SCC, it's
									// useless to
									// update it
									if (sccIsFinal.get(neighbor) == 0)
									{
										decreaseInDegree(neighbor);
										queueNext.add(neighbor);
										// System.out.println(" local vertex " +
										// neighbor + " queued.");
									}
								}
								else
								{
									remoteSend++;
									remoteUpdateDegrees(neighbor, vertex);
								}
							}
							for (LongCursor nCursor : gTopology.getInNeighbors(vertex))
							{
								long neighbor = nCursor.value;
								if (allocation.isLocalElement(neighbor))
								{
									if (sccIsFinal.get(neighbor) == 0)
									{
										decreaseOutDegree(neighbor);
										queueNext.add(neighbor);
										// System.out.println(" local vertex " +
										// neighbor + " queued.");
									}
								}
								else
								{
									remoteSend++;
									remoteUpdateDegrees(neighbor, vertex);
								}
							}
						}
					}
					vertexProcessed++;
				}
				while (remoteQueued + remoteIgnored < remoteReceived)
				{
					try
					{
						Thread.sleep(10);
					}
					catch (InterruptedException e)
					{
						e.printStackTrace();
					}
					;
				}
				synchronized (queueLock)
				{
					// Clear the queue as all the vertices have been processed
					queue.clear();
				}
				exchangeQueues();
			}
			// System.out.println("local computation done.");
			return new TrimPhaseInfos(vertexProcessed, sccAssigned, remoteSend,
					remoteReceived, remoteQueued, remoteIgnored, queueNext.size());
		}

		private void decreaseInDegree(long vertex)
		{
			synchronized (inDegrees)
			{
				int currentDegree = inDegrees.get(vertex);
				if (currentDegree > 0)
					inDegrees.put(vertex, currentDegree - 1);
			}
		}

		private void decreaseOutDegree(long vertex)
		{
			synchronized (outDegrees)
			{
				int currentDegree = outDegrees.get(vertex);
				if (currentDegree > 0)
					outDegrees.put(vertex, currentDegree - 1);
			}
		}

		private void exchangeQueues()
		{
			synchronized (queueLock)
			{
				// Exchange queue and queueNext.
				LongOpenHashSet tmp = queue;
				queue = queueNext;
				queueNext = tmp;
			}

		}

		private void remoteUpdateDegrees(long vertex, long newVertexInSCC)
		{
			// System.out.println("Sending [" + vertex + " " + newVertexInSCC +
			// "]");
			OctojusNode node = allocation.getOwnerNode(vertex);
			TCPConnection connection = connectTo(adjustDegrees, node);
			try
			{
				connection.out.writeLong(vertex);
				connection.out.writeLong(newVertexInSCC);
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}

		}

		private Service adjustDegrees = new Service()
		{
			@Override
			public void serveThrough(FullDuplexDataConnection2 c)
					throws ClassNotFoundException, IOException
			{
				long localVertex = c.in.readLong();
				long newVertexInSCC = c.in.readLong();
				remoteReceived++;
				// System.out.println("Received [" + localVertex + " " +
				// newVertexInSCC + "]");
				if (sccIsFinal.get(localVertex) == 0)
				{
					if (gTopology.getInNeighbors(localVertex).contains(newVertexInSCC))
					{
						decreaseInDegree(localVertex);
					}
					if (gTopology.getOutNeighbors(localVertex).contains(newVertexInSCC))
					{
						decreaseOutDegree(localVertex);
					}
					synchronized (queueLock)
					{
						queueNext.add(localVertex);
						remoteQueued++;
					}
					// System.out.println("vertex " + localVertex + " queued.");
				}
				else
				{
					remoteIgnored++;
				}
			}
		};

		@SuppressWarnings("serial")
		static class TrimPhaseInfos implements Serializable
		{
			private int vertexProcessed = 0;
			private int sccAssigned = 0;
			private int remoteSend = 0;
			private int remoteReceived = 0;
			private int remoteQueued = 0;
			private int remoteIgnored = 0;
			private int nextQueueSize = 0;

			public TrimPhaseInfos(int vertexProcessed, int sccAssigned, int remoteSend,
					int remoteReceived, int remoteQueued, int remoteIgnored,
					int nextQueueSize)
			{
				this.vertexProcessed = vertexProcessed;
				this.sccAssigned = sccAssigned;
				this.remoteSend = remoteSend;
				this.remoteReceived = remoteReceived;
				this.remoteQueued = remoteQueued;
				this.remoteIgnored = remoteIgnored;
				this.nextQueueSize = nextQueueSize;
			}

			@SuppressWarnings("unused")
			public TrimPhaseInfos()
			{
			}

			public TrimPhaseInfos add(TrimPhaseInfos other)
			{
				vertexProcessed += other.vertexProcessed;
				sccAssigned += other.sccAssigned;
				remoteSend += other.remoteSend;
				remoteReceived += other.remoteReceived;
				remoteQueued += other.remoteQueued;
				remoteIgnored += other.remoteIgnored;
				nextQueueSize += other.nextQueueSize;
				return this;
			}

			@Override
			public String toString()
			{
				return "#vertexProcessed: " + Integer.toString(vertexProcessed)
						+ " #sccAssigned: " + Integer.toString(sccAssigned)
						+ " #remoteSend: " + Integer.toString(remoteSend)
						+ " #remoteReceived: " + Integer.toString(remoteReceived)
						+ " #remoteQueued: " + Integer.toString(remoteQueued)
						+ " #remoteIgnored: " + Integer.toString(remoteIgnored)
						+ " nextQSize: " + Integer.toString(nextQueueSize);
			}
		}

		@SuppressWarnings("serial")
		private static class SingletonSCCExecutionRequest extends
				BigObjectComputingRequest<SingleVertexComponentsTrim, TrimPhaseInfos>
		{
			boolean verbose = false;

			public SingletonSCCExecutionRequest(String bigObjectID, boolean verbose)
			{
				super(bigObjectID);
				this.verbose = verbose;
			}

			@Override
			protected TrimPhaseInfos localComputation(SingleVertexComponentsTrim g)
					throws Throwable
			{
				return g.localComputation(verbose);
			}
		}
	}

	static public class IterativeSCC
			extends LiveDistributedObject<Serializable, Serializable>
	{

		GraphTopology inputGraph;
		private DHTAllocation allocation;

		private LongDHT sccFinalIds;

		private LongIntOpenHashMap lowLinks;
		private int currentIndex = 0;
		private int sccLabel;

		private IntLongOpenHashMap labelToSCCId;

		private final boolean useLinkedStack = true;
		private VertexStack dfsStack;

		private VertexStack backtrackStack;

		public static final String ITERATIVE_SCC_ID_SUFFIX = "/LocalIterativeSCC";

		public IterativeSCC(GraphTopology graph, LongDHT sccFinalsIds)
		{
			this(graph.getID() + ITERATIVE_SCC_ID_SUFFIX, graph.getAllocation(),
					graph.getID(), sccFinalsIds.getID());
		}

		IterativeSCC(String id, DHTAllocation allocation, String graphID, String sccID)
		{
			super(id, allocation, graphID, sccID);
			inputGraph = (GraphTopology) BigObjectRegistry.defaultRegistry.get(graphID);
			this.allocation = allocation;
			sccFinalIds = (LongDHT) BigObjectRegistry.defaultRegistry.get(sccID);

			int nLocalVertices = inputGraph.getNumberOfLocalVertices();
			lowLinks = new LongIntOpenHashMap(nLocalVertices);
			if (useLinkedStack)
			{
				dfsStack = new LinkedStack(nLocalVertices);
			}
			else
			{
				dfsStack = new ArrayStack(nLocalVertices);
			}
			backtrackStack = new ArrayStack(nLocalVertices);
			sccLabel = nLocalVertices;
			labelToSCCId = new IntLongOpenHashMap(nLocalVertices);
		}

		public Map<OctojusNode, LocalSCCResults> execute(Collection<OctojusNode> nodes)
		{
			return new OneNodeOneRequest<LocalSCCResults>()
			{
				@Override
				protected ComputationRequest<LocalSCCResults> createComputationRequestForNode(
						OctojusNode node)
				{
					return new LocalIterativeSCCExecuteRequest(IterativeSCC.this.getID());
				}
			}.execute(nodes);
		}

		LocalSCCResults execute()
		{
			LocalSCCResults infos = new LocalSCCResults();
			for (LongCursor elt : LongCursor.fromHPPC(inputGraph.getLocalMap().keys()))
			{
				long vertex = elt.value;
				if ( ! lowLinks.containsKey(vertex))
				{
					dfsStack.push(vertex);
					iterativeSCCDfs(infos);
				}
			}
			// sccFinalIds contains internal label value, not the expected min
			// is of vertices in the SCC
			for (LongCursor elt : LongCursor.fromHPPC(inputGraph.getLocalMap().keys()))
			{
				long vertex = elt.value;
				int label = (int) sccFinalIds.get(vertex);
				sccFinalIds.set(vertex, labelToSCCId.get(label));
			}
			return infos;
		}

		private void iterativeSCCDfs(LocalSCCResults infos)
		{
			while ( ! dfsStack.isEmpty())
			{
				long vertex = dfsStack.top();
				if ( ! lowLinks.containsKey(vertex))
				{
					lowLinks.put(vertex, currentIndex++);
					for (LongCursor elt : inputGraph.getOutNeighbors(vertex))
					{
						long neighbor = elt.value;
						if (allocation.isLocalElement(neighbor)
								&& ( ! lowLinks.containsKey(neighbor)))
						{
							if (dfsStack.isOnStack(neighbor))
							{
								dfsStack.remove(neighbor);
							}
							dfsStack.push(neighbor);
						}
					}
				}
				else
				{
					dfsStack.pop();
					infos.verticesProcessed++;

					boolean root = true;
					int low_v = lowLinks.get(vertex);
					for (LongCursor elt : inputGraph.getOutNeighbors(vertex))
					{
						long neighbor = elt.value;
						if (allocation.isLocalElement(neighbor)
								&& ( ! sccFinalIds.containsKey(neighbor)))
						{
							int low_w = lowLinks.get(neighbor);
							if (low_w < low_v)
							{
								low_v = low_w;
								root = false;
							}
						}
					}
					lowLinks.put(vertex, low_v);

					if (root)
					{
						low_v = lowLinks.get(vertex);
						long minVertexId = vertex;
						int sccSize = 0;
						while (( ! backtrackStack.isEmpty())
								&& low_v <= lowLinks.get(backtrackStack.top()))
						{
							long w = backtrackStack.pop();
							sccFinalIds.set(w, sccLabel);
							currentIndex -= 1;
							minVertexId = Math.min(minVertexId, w);
							sccSize++;
						}
						sccFinalIds.set(vertex, sccLabel);
						currentIndex -= 1;
						sccSize++;
						labelToSCCId.put(sccLabel, minVertexId);
						sccLabel -= 1;
						if (infos.largestSCCSize < sccSize)
						{
							infos.largestSCCId = minVertexId;
							infos.largestSCCSize = sccSize;
						}
						infos.sccFound += 1;
					}
					else
					{
						backtrackStack.push(vertex);
					}
				}
			}
		}

		interface VertexStack
		{
			public void push(long vertex);

			public long top();

			public long pop();

			public void remove(long vertex);

			public boolean isEmpty();

			public boolean isOnStack(long vertex);
		}

		class LinkedStack implements VertexStack
		{
			private LongLongOpenHashMap stackFwd;
			private LongLongOpenHashMap stackBwd;
			private long stackHead;
			private final long STACK_END = - 1;

			public LinkedStack(int capacity)
			{
				stackFwd = new LongLongOpenHashMap(capacity);
				stackBwd = new LongLongOpenHashMap(capacity);
				stackHead = STACK_END;
			}

			@Override
			public void push(long vertex)
			{
				stackFwd.put(vertex, stackHead);
				stackBwd.put(stackHead, vertex);
				stackBwd.put(vertex, STACK_END);
				stackHead = vertex;
			}

			@Override
			public long top()
			{
				return stackHead;
			}

			@Override
			public long pop()
			{
				long top = stackHead;
				stackHead = stackFwd.get(stackHead);
				stackBwd.put(stackHead, STACK_END);
				stackFwd.put(top, STACK_END);
				stackBwd.put(top, STACK_END);
				return top;
			}

			@Override
			public void remove(long vertex)
			{
				long eltBefore = stackBwd.get(vertex);
				stackFwd.put(eltBefore, stackFwd.get(vertex));
				long eltAfter = stackFwd.get(vertex);
				stackBwd.put(eltAfter, stackBwd.get(vertex));
				stackFwd.put(vertex, STACK_END);
				stackBwd.put(vertex, STACK_END);
			}

			@Override
			public boolean isEmpty()
			{
				return stackHead == STACK_END;
			}

			@Override
			public boolean isOnStack(long vertex)
			{
				return stackFwd.containsKey(vertex) && stackFwd.lget() != STACK_END;
			}
		}

		class ArrayStack implements VertexStack
		{
			private LongByteOpenHashMap onStack;
			private long[] stackArray;
			private int stackPtr = 0;

			public ArrayStack(int capacity)
			{
				stackArray = new long[capacity];
				onStack = new LongByteOpenHashMap(capacity);
			}

			@Override
			public void push(long vertex)
			{
				stackArray[stackPtr] = vertex;
				stackPtr += 1;
				onStack.put(vertex, (byte) 1);
			}

			@Override
			public long top()
			{
				return stackArray[stackPtr - 1];
			}

			@Override
			public long pop()
			{
				stackPtr -= 1;
				long top = stackArray[stackPtr];
				onStack.put(top, (byte) 0);
				return top;
			}

			@Override
			public void remove(long vertex)
			{
				int index;
				for (index = 0; index < stackPtr; index++)
				{
					if (stackArray[index] == vertex)
					{
						break;
					}
				}
				if (index < stackPtr)
				{
					for (int j = index; j < stackPtr - 1; j++)
					{
						stackArray[j] = stackArray[j + 1];
					}
					stackPtr -= 1;
					onStack.put(vertex, (byte) 0);
				}
			}

			@Override
			public boolean isEmpty()
			{
				return stackPtr == 0;
			}

			@Override
			public boolean isOnStack(long vertex)
			{
				return onStack.get(vertex) != 0;
			}
		}

		@Override
		public void clearLocalData()
		{
		}

		@SuppressWarnings("serial")
		static private class LocalIterativeSCCExecuteRequest
				extends BigObjectComputingRequest<IterativeSCC, LocalSCCResults>
		{
			public LocalIterativeSCCExecuteRequest(String bigObjectID)
			{
				super(bigObjectID);
			}

			@Override
			protected LocalSCCResults localComputation(IterativeSCC object)
					throws Throwable
			{
				return object.execute();
			}
		}

	}

	static public class TarjanSCC
			extends LiveDistributedObject<Serializable, Serializable>
	{
		GraphTopology inputGraph;
		private DHTAllocation allocation;

		private LongDHT sccFinalIds;

		LongIntOpenHashMap indices;
		LongIntOpenHashMap lowLinks;
		LongByteOpenHashMap onStack;

		public static final String LOCALTARJAN_ID_SUFFIX = "/LocalTarjanSCC";

		public TarjanSCC(GraphTopology graph, LongDHT sccFinalsIds)
		{
			this(graph.getID() + LOCALTARJAN_ID_SUFFIX, graph.getAllocation(),
					graph.getID(), sccFinalsIds.getID());
		}

		TarjanSCC(String id, DHTAllocation allocation, String graphID, String sccID)
		{
			super(id, allocation, graphID, sccID);
			inputGraph = (GraphTopology) BigObjectRegistry.defaultRegistry.get(graphID);
			this.allocation = allocation;
			sccFinalIds = (LongDHT) BigObjectRegistry.defaultRegistry.get(sccID);

			int nLocalVertices = inputGraph.getNumberOfLocalVertices();
			indices = new LongIntOpenHashMap(nLocalVertices);
			lowLinks = new LongIntOpenHashMap(nLocalVertices);
			onStack = new LongByteOpenHashMap(nLocalVertices);
			stack = new long[nLocalVertices];
		}

		public Map<OctojusNode, LocalSCCResults> execute(Collection<OctojusNode> nodes)
		{
			return new OneNodeOneRequest<LocalSCCResults>()
			{
				@Override
				protected ComputationRequest<LocalSCCResults> createComputationRequestForNode(
						OctojusNode node)
				{
					return new LocalTarjanExecuteRequest(TarjanSCC.this.getID());
				}
			}.execute(nodes);
		}

		public LocalSCCResults execute()
		{
			LocalSCCResults infos = new LocalSCCResults();
			for (LongCursor elt : LongCursor.fromHPPC(inputGraph.getLocalMap().keys()))
			{
				long vertex = elt.value;
				if ( ! indices.containsKey(vertex))
				{
					strongConnect(vertex, infos);
				}
			}
			return infos;
		}

		private int currentIndex = 0;
		private long[] stack;
		private int stackPtr = 0;

		private void strongConnect(long vertex, LocalSCCResults infos)
		{
			indices.put(vertex, currentIndex);
			lowLinks.put(vertex, currentIndex);
			currentIndex++;
			push(vertex);
			onStack.put(vertex, (byte) 1);

			infos.verticesProcessed++;

			for (LongCursor elt : inputGraph.getOutNeighbors(vertex))
			{
				long neighbor = elt.value;
				if (allocation.isLocalElement(neighbor))
				{
					if ( ! indices.containsKey(neighbor))
					{
						strongConnect(neighbor, infos);
						lowLinks.put(vertex,
								Math.min(lowLinks.get(vertex), lowLinks.get(neighbor)));
					}
					else if (onStack.get(neighbor) != 0)
					{
						lowLinks.put(vertex,
								Math.min(lowLinks.get(vertex), indices.get(neighbor)));
					}
				}
			}

			if (lowLinks.get(vertex) == indices.get(vertex))
			{
				long sccElement;
				// simulate the succession of pop() only to enumerate the
				// vertices and get
				// the lowest id
				int ptr = stackPtr;
				long minId = Long.MAX_VALUE;
				do
				{
					sccElement = stack[--ptr];
					if (sccElement < minId)
					{
						minId = sccElement;
					}
				}
				while (sccElement != vertex);
				// Do the actual removal from the stackArray and assign the
				// vertices to the
				// SCC numbered minId;
				int sccSize = 0;
				do
				{
					sccElement = pop();
					onStack.put(sccElement, (byte) 0);
					sccFinalIds.set(sccElement, minId);
					sccSize++;
				}
				while (sccElement != vertex);
				infos.sccFound++;
				infos.verticesInSCC += sccSize;
				if (sccSize > infos.largestSCCSize)
				{
					infos.largestSCCSize = sccSize;
					infos.largestSCCId = minId;
				}
			}
		}

		private void push(long vertex)
		{
			stack[stackPtr++] = vertex;
		}

		private long pop()
		{
			return stack[--stackPtr];
		}

		@Override
		public void clearLocalData()
		{
			stack = null;
			indices = null;
			lowLinks = null;
			onStack = null;
		}

		@SuppressWarnings("serial")
		static public class LocalSCCResults implements Serializable
		{
			int verticesProcessed = 0;
			int sccFound = 0;
			long verticesInSCC = 0;
			long largestSCCId = 0;
			int largestSCCSize = Integer.MIN_VALUE;

			public int getAvgSCCSize()
			{
				return (int) (sccFound > 0 ? (verticesInSCC / sccFound) : 0);
			}

			@Override
			public String toString()
			{
				return Integer.toString(sccFound) + " SCCs, avg. Size = "
						+ getAvgSCCSize() + ", largest = " + largestSCCId + " ("
						+ largestSCCSize + " vert.), " + verticesProcessed
						+ " vert. processed.";
			}
		}

		@SuppressWarnings("serial")
		static private class LocalTarjanExecuteRequest
				extends BigObjectComputingRequest<TarjanSCC, LocalSCCResults>
		{
			public LocalTarjanExecuteRequest(String bigObjectID)
			{
				super(bigObjectID);
			}

			@Override
			protected LocalSCCResults localComputation(TarjanSCC object) throws Throwable
			{
				return object.execute();
			}
		}
	}

	static public class GraphConverter
			extends LiveDistributedObject<LongLongOpenHashMap, Serializable>
	{
		private GraphTopology inputGraph;
		private GraphTopology outputGraph;
		private LongDHT verticesMap;

		private LongLongOpenHashMap remoteCache;
		private volatile boolean cacheFilled = false;

		boolean verbose = false;

		public GraphConverter(GraphTopology inputGraph, LongDHT verticesMap,
				GraphTopology outputGraph, boolean verbose)
		{
			this(inputGraph.getID() + "/graphConverter", inputGraph.getAllocation(),
					inputGraph.getID(), verticesMap.getID(), outputGraph.getID(),
					verbose);
		}

		protected GraphConverter(String id, Allocation a, String inputGraphID,
				String verticesMapId, String outputGraphID, Boolean verbose)
		{
			super(id, a, inputGraphID, verticesMapId, outputGraphID, verbose);
			inputGraph = (GraphTopology) BigObjectRegistry.defaultRegistry
					.get(inputGraphID);
			verticesMap = (LongDHT) BigObjectRegistry.defaultRegistry.get(verticesMapId);
			outputGraph = (GraphTopology) BigObjectRegistry.defaultRegistry
					.get(outputGraphID);
			remoteCache = new LongLongOpenHashMap();
			this.verbose = verbose.booleanValue();
		}

		@Override
		public void clearLocalData()
		{
			// remoteCache.clear();
			remoteCache = null;
		}

		private long localConvertionCount = 0;
		private long remoteConvertionCount = 0;
		private long remoteConvertionCacheMiss = 0;

		void execute()
		{
			new OneNodeOneRequest<NoReturn>()
			{
				@Override
				protected ComputationRequest<NoReturn> createComputationRequestForNode(
						OctojusNode n)
				{
					return new ExecutionRequest(getID());
				}
			}.execute(inputGraph.getAllocation().getNodes());
		}

		@SuppressWarnings("serial")
		static private class ExecutionRequest
				extends BigObjectComputingRequest<GraphConverter, NoReturn>
		{
			public ExecutionRequest(String bigObjectID)
			{
				super(bigObjectID);
			}

			@Override
			protected NoReturn localComputation(GraphConverter converter) throws Throwable
			{
				converter.localExecute();
				return null;
			}
		}

		private void localExecute()
		{
			StopWatch sw = new StopWatch(UNIT.ms);
			Thread fillCacheThread = new Thread(new Runnable()
			{

				@Override
				public void run()
				{
					fillCache();
				}
			}, "FillCacheThread");
			cacheFilled = false;
			fillCacheThread.start();

			DHTAllocation allocation = (DHTAllocation) getAllocation();
			LongObjectOpenHashMap<LongSet> outMap = outputGraph.getOutAdjacencyTable()
					.getLocalMap();
			// Iterate on the OUT adjacency table
			if (verbose)
			{
				System.out.println("Converting OUT adjacency table.");
			}
			int nElts = inputGraph.getOutAdjacencyTable().getLocalMap().size();
			int verboseProgressStep = Math.max(100 * 1000,
					Math.min(10 * 1000 * 1000, (nElts / 10000) * 1000));
			int eltCount = 0;
			for (LongObjectCursor<LongSet> elt : inputGraph.getOutAdjacencyTable()
					.getLocalMap())
			{
				long vertex = elt.key;
				LongSet neighbors = elt.value;
				int nNeighbors = neighbors.size();
				long newVertex = verticesMap.get(vertex);
				// Avoid duplicate edges by using a set
				LongSet newNeighbors = outMap.get(newVertex);
				if (newNeighbors == null)
				{
					newNeighbors = new HashLongSet64(nNeighbors);
					outMap.put(newVertex, newNeighbors);
				}
				for (LongCursor nelt : neighbors)
				{
					long neighbor = nelt.value;
					long newNeighbor;
					if (allocation.isLocalElement(neighbor))
					{
						newNeighbor = verticesMap.get(neighbor);
						localConvertionCount += 1;
					}
					else
					{
						newNeighbor = convertRemoteVertex(neighbor);
					}
					// No self-edge
					if (newNeighbor != newVertex)
					{
						newNeighbors.add(newNeighbor);
					}
				}
				eltCount += 1;
				if (verbose && eltCount % verboseProgressStep == 0)
				{
					System.out.println("Processed " + eltCount + " vertices: "
							+ localConvertionCount + " local vertex conv. "
							+ remoteConvertionCount + " remote vertex conv. (cache miss: "
							+ remoteConvertionCacheMiss + ")");
				}
			}
			long elapsedTime = sw.getElapsedTime();
			if (verbose)
			{
				System.out.println("Graph converter: processed " + eltCount
						+ " vertices in " + elapsedTime + "ms, " + localConvertionCount
						+ " local vertex conv. " + remoteConvertionCount
						+ " remote vertex conv. (cache miss: " + remoteConvertionCacheMiss
						+ ")");
			}
			if (inputGraph.isBiDirectional())
			{
				// Iterate on the IN adjacency table
				if (verbose)
				{
					System.out.println("Converting IN adjacency table.");
				}
				sw.reset();
				LongObjectOpenHashMap<LongSet> inMap = outputGraph.getInAdjacencyTable()
						.getLocalMap();
				nElts = inputGraph.getInAdjacencyTable().getLocalMap().size();
				verboseProgressStep = Math.max(100 * 1000,
						Math.min(10 * 1000 * 1000, (nElts / 10000) * 1000));
				eltCount = 0;
				for (LongObjectCursor<LongSet> elt : inputGraph.getInAdjacencyTable()
						.getLocalMap())
				{
					long vertex = elt.key;
					LongSet neighbors = elt.value;
					int nNeighbors = neighbors.size();
					long newVertex = verticesMap.get(vertex);
					// Avoid duplicate edges by using a set
					LongSet newNeighbors = inMap.get(newVertex);
					if (newNeighbors == null)
					{
						newNeighbors = new HashLongSet64(nNeighbors);
						inMap.put(newVertex, newNeighbors);
					}
					for (LongCursor nelt : neighbors)
					{
						long neighbor = nelt.value;
						long newNeighbor;
						if (allocation.isLocalElement(neighbor))
						{
							newNeighbor = verticesMap.get(neighbor);
							localConvertionCount += 1;
						}
						else
						{
							newNeighbor = convertRemoteVertex(neighbor);
						}
						// No self-edge
						if (newNeighbor != newVertex)
						{
							newNeighbors.add(newNeighbor);
						}
					}
					eltCount += 1;
					if (verbose && eltCount % verboseProgressStep == 0)
					{
						System.out.println("Processed " + eltCount + " vertices: "
								+ localConvertionCount + " local vertex conv. "
								+ remoteConvertionCount
								+ " remote vertex conv. (cache miss: "
								+ remoteConvertionCacheMiss + ")");
					}
				}
				elapsedTime = sw.getElapsedTime();
			}
			try
			{
				fillCacheThread.join();
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
			if (verbose)
			{
				System.out.println("Graph converter: processed " + eltCount
						+ " vertices in " + elapsedTime + "ms, " + localConvertionCount
						+ " local vertex conv. " + remoteConvertionCount
						+ " remote vertex conv. (cache miss: " + remoteConvertionCacheMiss
						+ ")");
			}
		}

		private int remoteVertexScheduled = 0;
		private int localEdgeCount = 0;
		private int remoteEdgeCount = 0;
		private AtomicInteger vertexSent = new AtomicInteger(0);
		private AtomicInteger vertexReceived = new AtomicInteger(0);

		private ConcurrentHashMap<OctojusNode, LongOpenHashSet> hostTable = new ConcurrentHashMap<OctojusNode, LongOpenHashSet>();
		private int vertexSetCapacity = 1000;
		private int vertexSetTransmitThreshold = 1000;

		private boolean useThreadPool = true;
		private Map<OctojusNode, ExecutorService> threadPools = null;

		private void fillCache()
		{
			DHTAllocation allocation = (DHTAllocation) getAllocation();
			// Estimate an adequate value for the batch conversion of vertex id.
			int nLocalVertices = inputGraph.getNumberOfLocalVertices();
			int batchSize = nLocalVertices / 20;
			vertexSetTransmitThreshold = Math.min(1000 * 1000, Math.max(batchSize, 1000));
			vertexSetCapacity = vertexSetTransmitThreshold;
			for (OctojusNode node : allocation.getNodes())
			{
				if ( ! node.isLocalNode())
				{
					hostTable.put(node, new LongOpenHashSet(vertexSetCapacity));
				}
			}
			if (useThreadPool)
			{
				threadPools = new HashMap<OctojusNode, ExecutorService>();
				for (OctojusNode node : allocation.getNodes())
				{
					if ( ! node.isLocalNode())
					{
						threadPools.put(node, Executors.newSingleThreadExecutor());
					}
				}
			}
			StopWatch sw = new StopWatch(UNIT.ms);
			for (LongObjectCursor<LongSet> elt : inputGraph.getOutAdjacencyTable()
					.getLocalMap())
			{
				LongSet neighbors = elt.value;
				for (LongCursor nelt : neighbors)
				{
					long neighbor = nelt.value;
					if (allocation.isLocalElement(neighbor))
					{
						localEdgeCount++;
					}
					else
					{
						remoteEdgeCount++;
						if ( ! remoteCache.containsKey(neighbor))
						{
							scheduleVertexConversion(neighbor);
						}

					}
				}
			}
			if (inputGraph.isBiDirectional())
			{
				for (LongObjectCursor<LongSet> elt : inputGraph.getInAdjacencyTable()
						.getLocalMap())
				{
					LongSet neighbors = elt.value;
					for (LongCursor nelt : neighbors)
					{
						long neighbor = nelt.value;
						if ( ! allocation.isLocalElement(neighbor)
								&& ! remoteCache.containsKey(neighbor))
						{
							scheduleVertexConversion(neighbor);
						}
					}
				}
			}
			// Process all remaining vertex id not yet converted.
			for (Entry<OctojusNode, LongOpenHashSet> entry : hostTable.entrySet())
			{
				OctojusNode node = entry.getKey();
				LongOpenHashSet set = entry.getValue();
				if (set != null && set.size() > 0)
				{
					convertLongSet(set, node);
				}
			}
			hostTable.clear();
			long elapsedTimeMs = sw.getElapsedTime();
			if (verbose)
			{
				System.out.println("Cache fill process first step in " + elapsedTimeMs
						+ " ms: " + localEdgeCount + " local edges, " + remoteEdgeCount
						+ " remote edges," + remoteVertexScheduled
						+ " remote vertex scheduled, " + " sent " + vertexSent.get()
						+ ", received " + vertexReceived.get() + ", converted "
						+ vertexConverted.get());
			}
			sw.reset();
			if (useThreadPool)
			{
				for (ExecutorService threadPool : threadPools.values())
				{
					threadPool.shutdown();
				}
				if (vertexSent.get() < remoteVertexScheduled
						|| vertexSent.get() > vertexReceived.get())
				{
					try
					{
						boolean allTerminated = false;
						while (vertexSent.get() < remoteVertexScheduled
								|| vertexSent.get() > vertexReceived.get()
								|| ! allTerminated)
						{
							allTerminated = true;
							for (ExecutorService threadPool : threadPools.values())
							{
								if ( ! threadPool.awaitTermination(2, TimeUnit.SECONDS))
								{
									allTerminated = false;
									// Stop the loop, because there is no need
									// to check the
									// other thread pools, the condition we are
									// looking for is
									// that all thread pools are terminated.
									break;
								}
							}
						}
					}
					catch (InterruptedException ie)
					{
						ie.printStackTrace();
					}
				}
			}

			if (vertexSent.get() != vertexReceived.get())
			{
				System.err.println("Remote vertex cache fill process error: sent "
						+ vertexSent.get() + ", received " + vertexReceived.get());
			}
			cacheFilled = true;
			synchronized (remoteCache)
			{
				remoteCache.notify();
			}
			long terminatingTimeMs = sw.getElapsedTime();
			if (verbose)
			{
				System.out.println("Cache fill process terminated in " + elapsedTimeMs
						+ " ms + " + terminatingTimeMs + " ms: " + remoteVertexScheduled
						+ " remote vertex scheduled, " + "sent " + vertexSent.get()
						+ ", received " + vertexReceived.get() + ", converted "
						+ vertexConverted.get());
			}
		}

		private void scheduleVertexConversion(long vertex)
		{
			OctojusNode node = ((DHTAllocation) getAllocation()).getOwnerNode(vertex);
			LongOpenHashSet set = hostTable.get(node);
			if (set.add(vertex))
			{
				if (set.size() >= vertexSetTransmitThreshold)
				{
					convertLongSet(set, node);
				}
			}
		}

		private long convertRemoteVertex(long vertex)
		{
			remoteConvertionCount += 1;
			if (cacheFilled)
			{
				long res = remoteCache.get(vertex);
				return res;
			}
			try
			{
				synchronized (remoteCache)
				{
					while ( ! cacheFilled && ! remoteCache.containsKey(vertex))
					{
						remoteConvertionCacheMiss += 1;
						remoteCache.wait();
					}
					long res = remoteCache.get(vertex);
					return res;
				}
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
				return - 1;
			}
		}

		private void convertLongSet(final LongOpenHashSet set, final OctojusNode node)
		{
			remoteVertexScheduled += set.size();
			if (useThreadPool)
			{
				hostTable.put(node, new LongOpenHashSet(vertexSetCapacity));
				ExecutorService threadPool = threadPools.get(node);
				threadPool.execute(new Runnable()
				{
					@Override
					public void run()
					{
						synchronized (node)
						{
							useConvertionService(set, node);
							set.clear();
							synchronized (remoteCache)
							{
								remoteCache.notify();
							}
						}
					}
				});
			}
			else
			{
				useConvertionService(set, node);
				set.clear();
				synchronized (remoteCache)
				{
					remoteCache.notify();
				}
			}
		}

		private void useConvertionService(LongOpenHashSet set, OctojusNode node)
		{
			TCPConnection connection = connectTo(vertexMappingService, node);
			try
			{
				int nValues = set.size();
				connection.out.writeInt(nValues);
				for (LongCursor cursor : LongCursor.fromHPPC(set))
				{
					connection.out.writeLong(cursor.value);
					vertexSent.incrementAndGet();
				}
				connection.out.flush();
				long convertedValues[] = new long[nValues];
				for (int index = 0; index < nValues; index++)
				{
					long converted = connection.in.readLong();
					vertexReceived.incrementAndGet();
					convertedValues[index] = converted;
				}
				synchronized (remoteCache)
				{
					int index = 0;
					for (LongCursor cursor : LongCursor.fromHPPC(set))
					{
						long value = cursor.value;
						long converted = convertedValues[index];
						index += 1;
						remoteCache.put(value, converted);
					}
				}
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}

		private AtomicInteger vertexConverted = new AtomicInteger(0);

		private Service vertexMappingService = new Service()
		{
			@Override
			public void serveThrough(FullDuplexDataConnection2 connection)
					throws ClassNotFoundException, IOException
			{
				synchronized (connection)
				{
					int nElts = connection.in.readInt();
					long values[] = new long[nElts];
					for (int i = 0; i < nElts; i++)
					{
						values[i] = connection.in.readLong();
					}
					for (int i = 0; i < nElts; i++)
					{
						long newValue = verticesMap.get(values[i]);
						connection.out.writeLong(newValue);
						vertexConverted.incrementAndGet();
					}
					connection.out.flush();
				}
			}
		};
	}

	static class LongDHTComposer extends LiveDistributedObject<Serializable, Serializable>
	{
		private LongDHT left;
		private LongDHT right;
		private LongDHT result;

		/**
		 * result[v] = right[left[v]] for all v in left.
		 * 
		 * @param left
		 * @param right
		 * @param result
		 */
		public LongDHTComposer(LongDHT left, LongDHT right, LongDHT result)
		{
			this(left.getID() + "/compose/" + right.getID(), left.getAllocation(),
					left.getID(), right.getID(), result.getID());
		}

		protected LongDHTComposer(String id, DHTAllocation allocation, String leftId,
				String rightId, String resultID)
		{
			super(id, allocation, leftId, rightId, resultID);
			left = (LongDHT) BigObjectRegistry.defaultRegistry.get(leftId);
			right = (LongDHT) BigObjectRegistry.defaultRegistry.get(rightId);
			result = (LongDHT) BigObjectRegistry.defaultRegistry.get(resultID);
			localExecute();
		}

		@Override
		public void clearLocalData()
		{
		}

		void localExecute()
		{
			for (LongLongCursor elt : left.getLocalMap())
			{
				long v = elt.key;
				long val = elt.value; // left[v]
				long val2 = right.get(val); // right[left[v]]
				result.set(v, val2);
			}
		}
	}

}
