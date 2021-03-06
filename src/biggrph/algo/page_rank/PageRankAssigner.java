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

package biggrph.algo.page_rank;

import ldjo.bsp.BSPComputation;
import ldjo.bsp.Box;
import ldjo.bsp.ListBox;
import ldjo.bsp.SuperStepInfo;
import ldjo.bsp.BSPThreadSpecifics;
import octojus.NoReturn;
import toools.thread.MultiThreadPolicy;
import biggrph.BigAdjacencyTable;
import biggrph.GraphTopology;
import bigobject.BigObjectRegistry;
import dht.DHTAllocation;
import dht.LongDHT;
import dht.set.LongSet;

public class PageRankAssigner extends
		BSPComputation<BigAdjacencyTable, PageRankMessage, NoReturn>
{
	public LongDHT computedRanks;

	@SuppressWarnings("unchecked")
	public PageRankAssigner(GraphTopology topology, MultiThreadPolicy mtp)
	{
		this(topology.getID() + "/bsp-pageRank", topology.getAllocation(), topology
				.getID(), PageRankMessage.class,
				(Class<? extends Box<PageRankMessage>>) ListBox.class, false, mtp);
	}

	public PageRankAssigner(String id, DHTAllocation p, String graphID,
			Class<PageRankMessage> messageClass,
			Class<? extends Box<PageRankMessage>> boxClass,
			boolean asynchronousMessaging, MultiThreadPolicy mtp)
	{
		super(id, p, graphID, messageClass, boxClass, asynchronousMessaging, mtp, defaultMessagePacketSize);

		if (p.getInitiatorNode().isLocalNode())
		{
			computedRanks = new LongDHT(getID() + "/ranks", p);
		}
	}

	public LongDHT getRankAssignments()
	{
		if (computedRanks == null)
		{
			computedRanks = (LongDHT) BigObjectRegistry.defaultRegistry.get(getID()
					+ "/ranks");
		}

		return computedRanks;
	}

	@Override
	protected void computeLocalElement(BigAdjacencyTable topology, long step, long v,
			Box<PageRankMessage> inbox, BSPThreadSpecifics<BigAdjacencyTable, PageRankMessage> ti)
	{
		float pageRank = Float.intBitsToFloat((int) getRankAssignments().get(v));

		final float d = 0.85f;
		float ratioSum = 0;

		for (PageRankMessage m : inbox)
		{
			ratioSum += m.ratio;
		}

		LongSet outNeighors = topology.get(v);
		int outDegree = outNeighors.size();

		if (outDegree > 0)
		{
			ratioSum += (pageRank / outDegree);
			pageRank = (1 - d) + d * ratioSum;
			getRankAssignments().set(v, Float.floatToIntBits(pageRank));
			PageRankMessage m = new PageRankMessage();
			m.ratio = (float) (pageRank / outDegree);
			ti.post(m, outNeighors);
		}
	}

	@Override
	protected boolean finished(long step, SuperStepInfo<NoReturn> globalResult)
	{
		return step == 30;
	}

	@Override
	protected NoReturn getLocalResult()
	{
		return null;
	}

}
