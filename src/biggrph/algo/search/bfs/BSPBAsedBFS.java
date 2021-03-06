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
import bigobject.BigObjectRegistry;
import dht.DHTAllocation;
import dht.LongDHT;
import dht.SerializableLongLongMap;
import ldjo.bsp.BSPComputation;
import ldjo.bsp.BSPListener;
import ldjo.bsp.Box;
import ldjo.bsp.SingleMessageBox;
import ldjo.bsp.BSPThreadSpecifics;
import ldjo.bsp.msg.EmptyMessage;
import octojus.NoReturn;
import toools.collection.bigstuff.longset.LongCursor;
import toools.thread.MultiThreadPolicy;
import toools.util.LongPredicate;

public final class BSPBAsedBFS
		extends BSPComputation<BigAdjacencyTable, EmptyMessage, NoReturn>
{
	private LongDHT distances;
	private final LongPredicate driver;

	public BSPBAsedBFS(BigAdjacencyTable topology, long src, MultiThreadPolicy p,
			LongPredicate driver)
	{
		this(topology, src, p, defaultMessagePacketSize, driver);
	}

	@SuppressWarnings("unchecked")
	public BSPBAsedBFS(BigAdjacencyTable topology, long src, MultiThreadPolicy p,
			int messagePacketSize, LongPredicate driver)
	{
		this(topology.getID() + "/bsp-bfs (distance only) from " + src,
				topology.getAllocation(), topology.getID(), EmptyMessage.class,
				(Class<? extends Box<EmptyMessage>>) SingleMessageBox.class, false, p,
				messagePacketSize, driver);

		distances = new LongDHT(getID() + "/distances", getAllocation(),
				topology.getNumberOfVertices());
		postInitMessage(EmptyMessage.instance, src);
	}

	private BSPBAsedBFS(String id, DHTAllocation segments, String graphID,
			Class<EmptyMessage> messageClass, Class<? extends Box<EmptyMessage>> boxClass,
			boolean asynchronousMessaging, MultiThreadPolicy p, int messagePacketSize,
			LongPredicate driver)
	{
		super(id, segments, graphID, messageClass, boxClass, asynchronousMessaging, p,
				messagePacketSize, driver);
		this.driver = driver;
	}

	public LongDHT getDistancesDistributedMap()
	{
		if (distances == null)
		{
			distances = (LongDHT) BigObjectRegistry.defaultRegistry
					.get(getID() + "/distances");
			int nLocalVertices = getTarget().getLocalMap().size();
			distances.setLocalData(new SerializableLongLongMap(nLocalVertices));
		}

		return distances;
	}

	@Override
	protected void computeLocalElement(BigAdjacencyTable topology, long superstep, long v,
			Box<EmptyMessage> inbox, BSPThreadSpecifics<BigAdjacencyTable, EmptyMessage> ti)
	{
		// if distance was not yet computed
		if ( ! getDistancesDistributedMap().getLocalMap().containsKey(v))
		{
			getDistancesDistributedMap().getLocalMap().put(v, superstep);

			// if (driver.continueAfterVisiting(v, superstep))
			{
				for (LongCursor neighborCursor : topology.get(v))
				{
					long neighbor = neighborCursor.value;

					if (driver.accept(neighbor))
					{
						ti.post(EmptyMessage.instance, neighbor);
					}
				}
			}
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

	public static BSPBAsedBFS compute(BigAdjacencyTable topology, long src,
			BSPListener<BigAdjacencyTable, EmptyMessage, NoReturn> listener,
			MultiThreadPolicy p, LongPredicate driver)
	{
		return compute(topology, src, listener, p, defaultMessagePacketSize, driver);
	}

	public static BSPBAsedBFS compute(BigAdjacencyTable topology, long src,
			BSPListener<BigAdjacencyTable, EmptyMessage, NoReturn> listener,
			MultiThreadPolicy p, int networkBufferSize, LongPredicate driver)
	{
		BSPBAsedBFS pr = new BSPBAsedBFS(topology, src, p, networkBufferSize, driver);

		if (listener != null)
			pr.getListeners().add(listener);

		pr.execute();
		// pr.delete();
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
	}

}