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

package biggrph;

import java.util.Map;

import octojus.ComputationRequest;
import octojus.OctojusNode;
import bigobject.BigObjectComputingRequest;
import bigobject.BigObjectMapReduce;

class MemoryFootPrintInBytes extends BigObjectMapReduce<BigAdjacencyTable, Long, Map<OctojusNode, Long>>
{

	public MemoryFootPrintInBytes(BigAdjacencyTable g)
	{
		super(g);
	}

	@Override
	protected ComputationRequest<Long> map(OctojusNode n, BigAdjacencyTable bo)
	{
		return new LMF(bo.getID());
	}

	@Override
	protected Map<OctojusNode, Long> reduce(Map<OctojusNode, Long> r)
	{
		return r;
	}

	@SuppressWarnings("serial")
	private static class LMF extends BigObjectComputingRequest<BigAdjacencyTable, Long>
	{

		public LMF(String id)
		{
			super(id);
		}

		@Override
		protected Long localComputation(BigAdjacencyTable g)
		{
			return g.__local_getMemoryFootprintInBytes();
		}

	}

}
