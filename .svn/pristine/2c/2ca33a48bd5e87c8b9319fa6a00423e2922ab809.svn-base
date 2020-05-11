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

public final class NumberOfVerticesPerNodeAlgorithm extends
		BigObjectMapReduce<BigAdjacencyTable, Integer, Map<OctojusNode, Integer>>
{

	public NumberOfVerticesPerNodeAlgorithm(BigAdjacencyTable g)
	{
		super(g);
	}

	@Override
	protected ComputationRequest<Integer> map(OctojusNode n, BigAdjacencyTable g)
	{
		return new NumberOfVerticesLocalExec(g.getID());
	}

	@Override
	protected Map<OctojusNode, Integer> reduce(Map<OctojusNode, Integer> r)
	{
		return r;
	}

	@SuppressWarnings("serial")
	public static class NumberOfVerticesLocalExec extends
			BigObjectComputingRequest<BigAdjacencyTable, Integer>
	{
		public NumberOfVerticesLocalExec(String id)
		{
			super(id);
		}

		@Override
		protected Integer localComputation(BigAdjacencyTable g)
		{
			return g.getLocalData().size();
		}
	}

	public static Map<OctojusNode, Integer> compute(BigAdjacencyTable g)
	{
		return new NumberOfVerticesPerNodeAlgorithm(g).execute();
	}

}