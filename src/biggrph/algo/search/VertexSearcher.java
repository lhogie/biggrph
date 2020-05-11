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

package biggrph.algo.search;

import java.util.Map;

import octojus.ComputationRequest;
import octojus.OctojusNode;
import toools.util.LongPredicate;
import biggrph.BigAdjacencyTable;
import bigobject.BigObjectComputingRequest;
import bigobject.BigObjectMapReduce;

import com.carrotsearch.hppc.cursors.LongCursor;

import dht.set.ArrayListLongSet64;
import dht.set.LongSet;

public class VertexSearcher
		extends BigObjectMapReduce<BigAdjacencyTable, LongSet, LongSet>
{
	private LongPredicate matcher;

	public VertexSearcher(BigAdjacencyTable cc, LongPredicate matcher)
	{
		super(cc);
		this.matcher = matcher;
	}

	@Override
	protected ComputationRequest<LongSet> map(OctojusNode n, BigAdjacencyTable g)
	{
		return new LocalCode(g.getID(), matcher);
	}

	@Override
	protected LongSet reduce(Map<OctojusNode, LongSet> nodeResults)
	{
		LongSet r = new ArrayListLongSet64();

		for (LongSet s : nodeResults.values())
		{
			r.addAll(s);
		}

		return r;
	}

	@SuppressWarnings("serial")
	private static class LocalCode
			extends BigObjectComputingRequest<BigAdjacencyTable, LongSet>
	{
		private LongPredicate matcher;

		public LocalCode(String id, LongPredicate matcher)
		{
			super(id);
			this.matcher = matcher;
		}

		@Override
		protected LongSet localComputation(BigAdjacencyTable adj)
		{
			LongSet r = new ArrayListLongSet64();

			for (LongCursor c : adj.getLocalVertices())
			{
				long v = c.value;

				if (matcher.accept(v))
				{
					r.add(v);
				}
			}

			return r;
		}
	}

}
