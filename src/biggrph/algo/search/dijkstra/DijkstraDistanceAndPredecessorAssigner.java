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

package biggrph.algo.search.dijkstra;

import java.util.Collection;

import ldjo.bsp.ArrayListBox;
import ldjo.bsp.BSPComputation;
import ldjo.bsp.BSPListener;
import ldjo.bsp.Box;
import ldjo.bsp.BSPThreadSpecifics;
import octojus.NoReturn;
import toools.collections.Arrays;
import toools.thread.MultiThreadPolicy;
import toools.thread.NCoresNThreadsPolicy;
import biggrph.GraphTopology;
import biggrph.Path;
import bigobject.BigObjectRegistry;

import com.carrotsearch.hppc.LongArrayList;

import dht.DHTAllocation;
import dht.LongDHT;
import dht.SerializableLongLongMap;

public final class DijkstraDistanceAndPredecessorAssigner extends
		BSPComputation<GraphTopology, DijkstraMessage, NoReturn> implements
		DistanceAndPredecessorAssigner
{
	private LongDHT distances;
	private LongDHT predecessors;
	private long src;

	@SuppressWarnings("unchecked")
	public DijkstraDistanceAndPredecessorAssigner(GraphTopology topology, long src)
	{
		this(topology.getID() + "/Dijkstra (distance+pred) from " + src, topology
				.getAllocation(), topology.getID(), DijkstraMessage.class,
				(Class<? extends Box<DijkstraMessage>>) ArrayListBox.class, false,
				new NCoresNThreadsPolicy());
		postInitMessage(new DijkstraMessage(0, - 1), src);
		this.src = src;
	}

	DijkstraDistanceAndPredecessorAssigner(String id, DHTAllocation segments,
			String graphID, Class<DijkstraMessage> messageClass,
			Class<? extends Box<DijkstraMessage>> boxClass,
			boolean asynchronousMessaging, MultiThreadPolicy mtp)
	{
		super(id, segments, graphID, messageClass, boxClass, asynchronousMessaging, mtp, defaultMessagePacketSize);

		if (segments.getInitiatorNode().isLocalNode())
		{
			distances = new LongDHT(getID() + "/distances", getAllocation());
			predecessors = new LongDHT(getID() + "/predecessors", getAllocation());
		}
	}

	@Override
	public LongDHT getDistancesMap()
	{
		if (distances == null)
		{
			distances = (LongDHT) BigObjectRegistry.defaultRegistry.get(getID()
					+ "/distances");
			distances.setLocalData(new SerializableLongLongMap(getTarget()
					.getOutAdjacencyTable().__local_getNumberOfLocalElements()));
		}

		return distances;
	}

	@Override
	public LongDHT getPredecessors()
	{
		if (predecessors == null)
		{
			predecessors = (LongDHT) BigObjectRegistry.defaultRegistry.get(getID()
					+ "/predecessors");
			predecessors.setLocalData(new SerializableLongLongMap(getTarget()
					.getOutAdjacencyTable().__local_getNumberOfLocalElements()));
		}

		return predecessors;
	}

	@Override
	protected void computeLocalElement(GraphTopology topology, long step, long v,
			Box<DijkstraMessage> inbox, BSPThreadSpecifics<GraphTopology, DijkstraMessage> ti)
	{
		final long initialDistance;
		initialDistance = getDistancesMap().getLocalData().containsKey(v) ? getDistancesMap()
				.getLocalData().get(v) : Long.MAX_VALUE;
		long bestPrecedessor = - 1;
		long bestDistance = initialDistance;

		for (DijkstraMessage m : inbox)
		{
			if (m.distance < bestDistance)
			{
				bestDistance = m.distance;
				bestPrecedessor = m.predecessor;
			}
		}

		if (bestDistance < initialDistance)
		{
			getDistancesMap().set(v, bestDistance);
			getPredecessors().set(v, bestPrecedessor);
			// synchronized (this)
			{
				if (topology.getOutNeighbors(v) == null)
					throw new IllegalStateException("" + v);
				ti.post(new DijkstraMessage(bestDistance + 1, v),
						topology.getOutNeighbors(v));
			}

		}
	}

	@Override
	public void combine(Box<DijkstraMessage> msgs, DijkstraMessage newMessage)
	{
		if (msgs.isEmpty())
		{
			msgs.add(newMessage);
		}
		else
		{
			DijkstraMessage m = msgs.get(0);

			if (newMessage.distance < m.distance)
			{
				m.distance = newMessage.distance;
				m.predecessor = newMessage.predecessor;
			}
		}
	}

	public static DijkstraDistanceAndPredecessorAssigner compute(GraphTopology g, long src)
	{
		return compute(g, src, null);
	}

	public static DijkstraDistanceAndPredecessorAssigner compute(GraphTopology g,
			long src, BSPListener<GraphTopology, DijkstraMessage, NoReturn> listener)
	{
		DijkstraDistanceAndPredecessorAssigner pr = new DijkstraDistanceAndPredecessorAssigner(
				g, src);

		if (listener != null)
		{
			pr.getListeners().add(listener);
		}

		pr.execute();
		return pr;
	}

	@Override
	protected NoReturn getLocalResult()
	{
		return null;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void declareChildren(Collection children)
	{
		super.declareChildren(children);
		children.add(distances);
		children.add(predecessors);
	}


}