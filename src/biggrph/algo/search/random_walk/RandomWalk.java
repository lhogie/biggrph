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

package biggrph.algo.search.random_walk;

import java.util.Collection;
import java.util.Random;

import biggrph.GraphTopology;
import bigobject.BigObjectRegistry;
import dht.DHTAllocation;
import dht.LongDHT;
import dht.SerializableLongLongMap;
import dht.set.LongSet;
import ldjo.bsp.ArrayListBox;
import ldjo.bsp.BSPComputation;
import ldjo.bsp.Box;
import ldjo.bsp.BSPThreadSpecifics;
import ldjo.bsp.msg.EmptyMessage;
import octojus.NoReturn;
import toools.thread.MultiThreadPolicy;
import toools.util.LongPredicate;

/*
 * This class implements a random walk on the given graph g. 
 * The random walk is an iterative process starting from a starting vertex src. At each step, the active vertex increments its own counter and it
 * randomly chooses a successor among its neighbors. 
 * This process terminates when a after a given number of iterations have been done.
 */
public class RandomWalk extends BSPComputation<GraphTopology, EmptyMessage, NoReturn>
{
	private LongDHT nbVisits;
	private final Random prng;
	private int nbIterations;
	private final LongPredicate filter;

	@SuppressWarnings("unchecked")
	public RandomWalk(GraphTopology gTopology, LongSet sources, MultiThreadPolicy p,
			LongPredicate filter, int nbIterations, Random prng)
	{
		this(gTopology.getID() + "/bsp-randomwalk", gTopology.getAllocation(), gTopology
				.getID(), EmptyMessage.class,
				(Class<? extends Box<EmptyMessage>>) ArrayListBox.class, false, p,
				filter, nbIterations, prng);
		nbVisits = new LongDHT(getID() + "/nbVisits", getAllocation());
		postInitMessage(EmptyMessage.instance, sources);
	}

	private RandomWalk(String id, DHTAllocation segments, String graphID,
			Class<EmptyMessage> messageClass,
			Class<? extends Box<EmptyMessage>> boxClass, boolean asynchronousMessaging,
			MultiThreadPolicy p, LongPredicate filter, int nbIterations, Random prng)
	{
		super(id, segments, graphID, messageClass, boxClass, asynchronousMessaging, p, defaultMessagePacketSize, filter, nbIterations, prng);
		this.filter = filter;
		this.nbIterations = nbIterations;
		this.prng = prng;
	}

	public LongDHT getNbVisitsDistributedMap()
	{
		if (nbVisits == null)
		{
			nbVisits = (LongDHT) BigObjectRegistry.defaultRegistry.get(getID()
					+ "/nbVisits");
			nbVisits.setLocalData(new SerializableLongLongMap(getTarget().getLocalMap()
					.size()));
		}

		return nbVisits;
	}

	@Override
	protected void computeLocalElement(GraphTopology gTopology, long superstep, long v,
			Box<EmptyMessage> inbox, BSPThreadSpecifics<GraphTopology, EmptyMessage> ti)
	{
		if (superstep >= nbIterations)
			return;

		// gets how many times vertex v was visited in the past
		long nbVisitOfV = getNbVisitsDistributedMap().getLocalMap().get(v);

		// if (prng.nextDouble() < probabilityToDropThisNode)

		// increments that number of visits by the number of messages received
		getNbVisitsDistributedMap().getLocalMap().put(v, nbVisitOfV + inbox.size());

		// gets the neighbors of vertex v
		LongSet neighbors = gTopology.getOutNeighbors(v);

		// if vertex v has neighbors
		if ( ! neighbors.isEmpty())
		{
			// for each walk currently arrived at this vertex
			for (int i = inbox.size(); i > 0; --i)
			{
				// picks a random neighbor
				long successor = neighbors.pickRandomElement(prng);

				if (filter == null || filter.accept(successor))
				{
					// and signals it for execution at the next stop of BSP
					ti.post(EmptyMessage.instance, successor);
				}
			}
		}
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
		children.add(nbVisits);
	}

}