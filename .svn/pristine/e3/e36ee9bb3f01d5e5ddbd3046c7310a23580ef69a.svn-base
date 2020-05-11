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

package biggrph._algo.count;

import java.util.Map;

import octojus.ComputationRequest;
import octojus.OctojusNode;
import biggrph.BigAdjacencyTable;
import bigobject.BigObjectComputingRequest;
import bigobject.BigObjectMapReduce;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import dht.set.LongSet;

public final class NumberOfEdgesAlgorithm extends BigObjectMapReduce<BigAdjacencyTable, Long, Long>
{

	public NumberOfEdgesAlgorithm(BigAdjacencyTable g)
	{
		super(g);
	}

	@Override
	protected ComputationRequest<Long> map(OctojusNode n, BigAdjacencyTable g)
	{
		return new NumberOfEdgesLocalExec(g.getID());
	}

	@Override
	protected Long reduce(Map<OctojusNode, Long> r)
	{
		long sum = 0;

		for (long e : r.values())
		{
			sum += e;
		}

		return sum;
	}

	@SuppressWarnings("serial")
	public static class NumberOfEdgesLocalExec extends BigObjectComputingRequest<BigAdjacencyTable, Long>
	{
		public NumberOfEdgesLocalExec(String id)
		{
			super(id);
		}

		@Override
		protected Long localComputation(BigAdjacencyTable g)
		{
			long n = 0;

			for (ObjectCursor<LongSet> c : g.getLocalData().values())
			{
				n += c.value.size();
			}

			return n;
		}
	}

	public static long compute(BigAdjacencyTable g)
	{
		return new NumberOfEdgesAlgorithm(g).execute();
	}

}