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

package biggrph.algo.search.bfs;

import java.util.Collection;

import biggrph.BigAdjacencyTable;
import bigobject.BigObjectReference;
import dht.DHTAllocation;
import dht.LongDHT;
import ldjo.bsp.BSPComputation;
import ldjo.bsp.BSPListener;
import ldjo.bsp.Box;
import ldjo.bsp.SingleMessageBox;
import ldjo.bsp.BSPThreadSpecifics;
import ldjo.bsp.msg.EmptyMessage;
import octojus.NoReturn;
import toools.thread.MultiThreadPolicy;

public class BSPBAsedBFSDistanceAssigner2 extends BSPComputation<BigAdjacencyTable, EmptyMessage, NoReturn>
{
	private BigObjectReference<BigAdjacencyTable> adj;
	private BigObjectReference<LongDHT> distancesDht;

	@SuppressWarnings("unchecked")
	public BSPBAsedBFSDistanceAssigner2(BigAdjacencyTable topology, long src, MultiThreadPolicy p,
			boolean computePredecessors)
	{
		this(topology.getID() + "/bsp-bfs (distance only) from " + src,
				topology.getAllocation(), topology.getID(), EmptyMessage.class,
				(Class<? extends Box<EmptyMessage>>) SingleMessageBox.class, false, p, defaultMessagePacketSize);

		postInitMessage(EmptyMessage.instance, src);
	}

	private BSPBAsedBFSDistanceAssigner2(String id, DHTAllocation allocation, String graphID,
			Class<EmptyMessage> messageClass, Class<? extends Box<EmptyMessage>> boxClass,
			boolean asynchronousMessaging, MultiThreadPolicy p, int messagePacketSize)
	{
		super(id, allocation, graphID, messageClass, boxClass, asynchronousMessaging, p, messagePacketSize);
		adj = new BigObjectReference<>(graphID);

		if (allocation.getInitiatorNode().isLocalNode())
		{
			new LongDHT(getID() + "/distances", getAllocation(),
					adj.get().getNumberOfVertices());
		}

		distancesDht = new BigObjectReference<>(getID() + "/distances");
	}

	@Override
	protected void computeLocalElement(BigAdjacencyTable topology, long superstep, long v,
			Box<EmptyMessage> inbox, BSPThreadSpecifics<BigAdjacencyTable, EmptyMessage> ti)
	{
		// if distance was not yet computed
		if ( ! distancesDht.get().getLocalMap().containsKey(v))
		{
			distancesDht.get().getLocalMap().put(v, superstep);
			ti.post(EmptyMessage.instance, topology.get(v));
		}
	}

	@Override
	public void combine(Box<EmptyMessage> msgs, EmptyMessage newMessage)
	{
		if (msgs.isEmpty())
		{
			msgs.add(newMessage);
		}
	}

	public static BSPBAsedBFSDistanceAssigner2 compute(BigAdjacencyTable topology, long src,
			BSPListener<BigAdjacencyTable, EmptyMessage, NoReturn> listener,
			MultiThreadPolicy p)
	{
		BSPBAsedBFSDistanceAssigner2 pr = new BSPBAsedBFSDistanceAssigner2(topology, src, p, false);

		if (listener != null)
			pr.getListeners().add(listener);

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
		children.add(distancesDht);
	}

	public LongDHT getDistancesDHT()
	{
		return distancesDht.get();
	}
}