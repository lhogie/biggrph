package biggrph.algo.search.bfs;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;

import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import biggrph.BigAdjacencyTable;
import bigobject.BigObjectComputingRequest;
import bigobject.BigObjectMapReduce;
import bigobject.BigObjectRegistry;
import dht.DHTAllocation;
import dht.LongDHT;
import dht.set.LongSet;
import ldjo.bsp.Box;
import ldjo.bsp.MessageCombiner;
import ldjo.bsp.concurrent.ConcurrentBSPComputation;
import ldjo.bsp.msg.EmptyMessage;
import octojus.ComputationRequest;
import octojus.NoReturn;
import octojus.OctojusNode;
import octojus.OneNodeOneRequest;
import toools.collection.bigstuff.longset.LongCursor;
import toools.math.Distribution;
import toools.thread.MultiThreadPolicy;
import toools.thread.NThreadsPolicy;

public class ConcurrentBSPBasedBFS extends ConcurrentBSPComputation<BigAdjacencyTable, EmptyMessage, Serializable>
{

	static private final String BIGOBJECT_ID_SUFFIX = "/distances";
	
	private NonBlockingHashMapLong<Integer> distances;
	private LongDHT distancesDHT;
	
	public ConcurrentBSPBasedBFS(BigAdjacencyTable topology, long src, final int nbThreads)
	{
		this(topology, src, nbThreads, 1000 * 1000);
	}
	
	public ConcurrentBSPBasedBFS(BigAdjacencyTable topology, long src, final int nbThreads, int messagePacketSize)
	{
		this(topology.getID() + "/cbsp-bfs from " + src, topology.getAllocation(), topology.getID(),
				EmptyMessage.class,
				(MessageCombiner<EmptyMessage>) (EmptyMessage m1, EmptyMessage m2) -> m1,
				new NThreadsPolicy(nbThreads),
				messagePacketSize);
		this.distancesDHT = new LongDHT(getID() + BIGOBJECT_ID_SUFFIX, getAllocation(), getTarget().getNumberOfVertices());
		postInitMessage(EmptyMessage.instance, src);
	}

	@SuppressWarnings("unchecked")
	private ConcurrentBSPBasedBFS(String id, DHTAllocation allocation, String graphID,
			Class<EmptyMessage> messageClass, MessageCombiner<EmptyMessage> messageCombiner,
			MultiThreadPolicy threadPolicy, int messagePacketSize)
	{
		this(id, allocation, graphID, messageClass, (Class<Box<EmptyMessage>>)getMessageBoxClass(messageCombiner, threadPolicy),
				messageCombiner, threadPolicy, messagePacketSize);
	}

	private ConcurrentBSPBasedBFS(String id, DHTAllocation allocation, String graphID,
			Class<EmptyMessage> messageClass, Class<? extends Box<EmptyMessage>> messageBoxClass,
			MessageCombiner<EmptyMessage> messageCombiner,
			MultiThreadPolicy threadPolicy, int messagePacketSize)
	{
		super(id, allocation, graphID, messageClass, messageBoxClass, messageCombiner, threadPolicy, messagePacketSize);
		this.distances = new NonBlockingHashMapLong<>(getTarget().__local_getNumberOfLocalElements());
	}
			
	@Override
	public void computeLocalElement(BigAdjacencyTable grph, long superstep, long vertex,
			Box<EmptyMessage> messages, ConcurrentBSPComputation<BigAdjacencyTable, EmptyMessage, Serializable>.BSPHelper hp)
	{
		if (distances.get(vertex) == null)
		{
			@SuppressWarnings("unused")
			EmptyMessage inMsg = messages.get(0);
			EmptyMessage outMsg = EmptyMessage.instance;
			distances.put(vertex, new Integer((int)superstep));
			LongSet neighbors = grph.get(vertex);
			long nbNeighbors = neighbors.size();
			long neighborProcessed = 0;
			for (LongCursor neighborCursor : neighbors)
			{
				long neighbor = neighborCursor.value;
				hp.post(outMsg, neighbor);
				neighborProcessed++;
				if (neighborProcessed >= nbNeighbors)
					break;
			}
		}
	}
	
//	@Override
//	protected void stepEndHook(int superStep)
//	{
//		super.stepEndHook(superStep);
//		System.out.println("Reinjected Messages: " + reinjectedMessages.longValue());
//	}
	
	@Override
	public void execute()
	{
		// Execute the BSP algorithm
		super.execute();
		// Copy the results in the LongDHT, do it in parallel
		// on all cluster nodes.
		new OneNodeOneRequest<NoReturn>()
		{
			@Override
			protected ComputationRequest<NoReturn> createComputationRequestForNode(OctojusNode n)
			{
				return new CopyDistancesTask(getID());
			}
		}.execute(getAllocation().getNodes());
	}
	
	@SuppressWarnings("serial")
	private static class CopyDistancesTask extends BigObjectComputingRequest<ConcurrentBSPBasedBFS, NoReturn>
	{
		public CopyDistancesTask(String bigObjectID)
		{
			super(bigObjectID);
		}

		@Override
		protected NoReturn localComputation(ConcurrentBSPBasedBFS cbfs) throws Throwable
		{
			if (cbfs.distancesDHT == null)
			{
				cbfs.distancesDHT = (LongDHT) BigObjectRegistry.defaultRegistry.get(cbfs.getID() + BIGOBJECT_ID_SUFFIX);
			}
			for (Entry<Long, Integer> entry : cbfs.distances.entrySet())
			{
				long vertex = entry.getKey();
				int distance = entry.getValue();
				cbfs.distancesDHT.getLocalMap().put(vertex, distance);
			}
			return null;
		}	
	}
	
	public LongDHT getDistancesDistributedMap()
	{
		return distancesDHT;
	}

	public Distribution<Long> getDistribution()
	{
		return new DistributionComputation(this).execute();
	}
	
	private Distribution<Long> computeLocalDistribution()
	{
		Distribution<Long> distribution = new Distribution<Long>();
		distances.forEach((Long vertex, Integer distance) -> {distribution.addOccurence((long)distance);});
		return distribution;
	}

	static private class DistributionComputation extends BigObjectMapReduce<ConcurrentBSPBasedBFS, Distribution<Long>, Distribution<Long>>
	{

		public DistributionComputation(ConcurrentBSPBasedBFS bfs)
		{
			super(bfs);
		}

		@Override
		protected ComputationRequest<Distribution<Long>> map(OctojusNode n,
				ConcurrentBSPBasedBFS bfs)
		{
			return new LocalDistributionComputation(bfs.getID());
		}

		@Override
		protected Distribution<Long> reduce(Map<OctojusNode, Distribution<Long>> resultMap)
		{
			Distribution<Long> res = new Distribution<Long>();
			for (Distribution<Long> dist : resultMap.values())
			{
				res = res.mergeInPlace(dist);
			}
			return res;
		}
		
		@SuppressWarnings("serial")
		static class LocalDistributionComputation extends BigObjectComputingRequest<ConcurrentBSPBasedBFS, Distribution<Long>>
		{
			public LocalDistributionComputation(String bigObjectID)
			{
				super(bigObjectID);
			}

			@Override
			protected Distribution<Long> localComputation(ConcurrentBSPBasedBFS bfs)
					throws Throwable
			{
				return bfs.computeLocalDistribution();
			}
		}
		
		
	}
	
}
