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

package biggrph.algo.topology_generator;

import java.io.IOException;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import octojus.ComputationRequest;
import octojus.OctojusNode;
import biggrph.BigAdjacencyTable;
import biggrph.BigGrphCluster;
import bigobject.BigObjectCluster;
import bigobject.BigObjectComputingRequest;
import bigobject.BigObjectMapReduce;

import com.carrotsearch.hppc.cursors.LongCursor;

import dht.allocation.BasicHashDHTAllocation;

public class Chain
{
	public static void chain(final BigAdjacencyTable g, final long size, final boolean connectChunks, final boolean cycle)
	{
		new BigObjectMapReduce<BigAdjacencyTable, RR, Object>(g)
		{

			@Override
			protected ComputationRequest<RR> map(OctojusNode n, BigAdjacencyTable bo)
			{
				LocalGeneration j = new LocalGeneration(g.getID());
				j.size = size;
				return j;
			}

			@Override
			protected Object reduce(Map<OctojusNode, RR> r)
			{
				List<RR> l = new ArrayList<RR>(r.values());

				if (connectChunks)
				{
					for (int i = 1; i < l.size(); ++i)
					{
						g.add(l.get(i - 1).last, l.get(i).first);
					}

					if (cycle)
					{
						g.add(l.get(l.size() - 1).last, l.get(0).first);
					}
				}

				return null;
			}
		}.execute();

	}

	@SuppressWarnings("serial")
	private static class RR implements Serializable
	{
		long first, last;
	}

	@SuppressWarnings("serial")
	public static class LocalGeneration extends BigObjectComputingRequest<BigAdjacencyTable, RR>
	{
		long size;

		public LocalGeneration(String graphID)
		{
			super(graphID);
		}

		@Override
		protected RR localComputation(BigAdjacencyTable g)
		{
			RR r = new RR();
			Iterator<LongCursor> i = g.getLocalData().keys().iterator();
			long pred = r.first = r.last = i.next().value;

			while (i.hasNext())
			{
				r.last = i.next().value;
				g.__local_add(pred, r.last);
				pred = r.last;
			}

			return r;
		}
	}

	public static void main(String[] args) throws UnknownHostException, IOException
	{
		BigObjectCluster c = BigGrphCluster.workstations("k5000", "tina");
		BasicHashDHTAllocation allocation = new BasicHashDHTAllocation(c);

		BigAdjacencyTable g = new BigAdjacencyTable("test", allocation, 0, false);
		System.out.println("generating topology");
		chain(g, 400, true, false);
		System.out.println("completed");
	}
}
