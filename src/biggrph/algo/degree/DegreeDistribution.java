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
import toools.math.DistributionForLongs;
import biggrph.BigAdjacencyTable;
import bigobject.BigObjectComputingRequest;
import bigobject.BigObjectMapReduce;

import com.carrotsearch.hppc.cursors.LongCursor;

public  class DegreeDistribution extends BigObjectMapReduce<BigAdjacencyTable, DistributionForLongs, DistributionForLongs>
{
	public DegreeDistribution(BigAdjacencyTable cc)
	{
		super(cc);
	}

	@Override
	protected ComputationRequest<DistributionForLongs> map(OctojusNode n, BigAdjacencyTable bo)
	{
		return new ComputeLocalDegreeDistribution(bo.getID());
	}

	@Override
	protected DistributionForLongs reduce(Map<OctojusNode, DistributionForLongs> nodeResults)
	{
		DistributionForLongs r = new DistributionForLongs();

		for (DistributionForLongs s : nodeResults.values())
		{
			for (long cc : s.getOccuringObjects())
			{
				r.addNOccurences(cc, s.getNumberOfOccurences(cc));
			}
		}

		return r;
	}

	@SuppressWarnings("serial")
	private static class ComputeLocalDegreeDistribution extends BigObjectComputingRequest<BigAdjacencyTable, DistributionForLongs>
	{

		public ComputeLocalDegreeDistribution(String id)
		{
			super(id);
		}

		@Override
		protected DistributionForLongs localComputation(BigAdjacencyTable g)
		{
			DistributionForLongs r = new DistributionForLongs();

			for (LongCursor c : g.getLocalData().keys())
			{
				long degree = g.getLocalData().get(c.value).size();
				r.addOccurence(degree);
			}

			return r;
		}
	}

}