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

package biggrph.algo.degree;

import java.util.Map;

import octojus.ComputationRequest;
import octojus.OctojusNode;
import toools.util.LongPredicate;
import biggrph.BigAdjacencyTable;
import bigobject.BigObjectComputingRequest;
import bigobject.BigObjectMapReduce;

import com.carrotsearch.hppc.cursors.LongCursor;

public final class MaxDegreeAlgorithm
		extends
		BigObjectMapReduce<BigAdjacencyTable, MaxDegreeAlgorithmResult, MaxDegreeAlgorithmResult>
{
	private LongPredicate predicate = null;

	public MaxDegreeAlgorithm(BigAdjacencyTable g)
	{
		super(g);
	}

	public MaxDegreeAlgorithm(BigAdjacencyTable graph, LongPredicate vertexPredicate)
	{
		super(graph);
		this.predicate = vertexPredicate;
	}

	@Override
	protected ComputationRequest<MaxDegreeAlgorithmResult> map(OctojusNode n,
			BigAdjacencyTable g)
	{
		return new SubGraphMaxDegreeLocalComputing(g.getID(), predicate);
	}

	@Override
	protected MaxDegreeAlgorithmResult reduce(
			Map<OctojusNode, MaxDegreeAlgorithmResult> map)
	{
		MaxDegreeAlgorithmResult globalResult = new MaxDegreeAlgorithmResult();
		globalResult.degree = - 1;

		for (MaxDegreeAlgorithmResult localResult : map.values())
		{
			if (localResult.degree > globalResult.degree)
			{
				globalResult.degree = localResult.degree;
				globalResult.vertex = localResult.vertex;
			}
		}

		return globalResult;
	}

	@SuppressWarnings("serial")
	public static class SubGraphMaxDegreeLocalComputing extends
			BigObjectComputingRequest<BigAdjacencyTable, MaxDegreeAlgorithmResult>
	{
		public SubGraphMaxDegreeLocalComputing(String id)
		{
			super(id);
		}

		public SubGraphMaxDegreeLocalComputing(String id, LongPredicate predicate2)
		{
			super(id);
			this.predicate = predicate2;
		}

		private LongPredicate predicate = null;

		@Override
		protected MaxDegreeAlgorithmResult localComputation(BigAdjacencyTable g)
		{
			MaxDegreeAlgorithmResult result = new MaxDegreeAlgorithmResult();
			result.degree = - 1;

			for (LongCursor c : g.getLocalData().keys())
			{
				long vertex = c.value;
				if (predicate == null || (predicate != null && predicate.accept(vertex)))
				{
					int degree = g.get(vertex).size();

					if (degree > result.degree)
					{
						result.degree = degree;
						result.vertex = vertex;
					}
				}
			}

			return result;
		}
	}

}