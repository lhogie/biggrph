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
import toools.math.IntegerSequenceAnalyzer;
import toools.util.LongPredicate;
import biggrph.BigAdjacencyTable;
import bigobject.BigObjectComputingRequest;
import bigobject.BigObjectMapReduce;

public final class NumberOfVerticesAlgorithm extends BigObjectMapReduce<BigAdjacencyTable, Integer, Long>
{
	private LongPredicate filter;

	public NumberOfVerticesAlgorithm(BigAdjacencyTable g)
	{
		this(g, null);
	}
	
	public NumberOfVerticesAlgorithm(BigAdjacencyTable g, LongPredicate filter)
	{
		super(g);
		this.filter = filter;
	}

	@Override
	protected ComputationRequest<Integer> map(OctojusNode n, BigAdjacencyTable g)
	{
		return new NumberOfVerticesLocalExec(g.getID(), filter);
	}

	@Override
	protected Long reduce(Map<OctojusNode, Integer> r)
	{
		IntegerSequenceAnalyzer a = new IntegerSequenceAnalyzer();
		a.addInts(r.values());
		return a.getSum();
	}

	@SuppressWarnings("serial")
	public static class NumberOfVerticesLocalExec extends BigObjectComputingRequest<BigAdjacencyTable, Integer>
	{
		private LongPredicate filter;
		
		public NumberOfVerticesLocalExec(String id)
		{
			this(id, null);
		}
		
		public NumberOfVerticesLocalExec(String id, LongPredicate filter)
		{
			super(id);
			this.filter = filter;
		}

		@Override
		protected Integer localComputation(BigAdjacencyTable g)
		{
			return(filter != null ? g.getLocalData().count(filter) : g.getLocalData().size());
		}
	}

	public static long compute(BigAdjacencyTable g)
	{
		return new NumberOfVerticesAlgorithm(g).execute();
	}
	
	public static long compute(BigAdjacencyTable g, LongPredicate filter)
	{
		return new NumberOfVerticesAlgorithm(g, filter).execute();
	}

}