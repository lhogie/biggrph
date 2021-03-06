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

package biggrph.algo;

import java.util.Map;

import bigobject.BigObjectComputingRequest;
import bigobject.BigObjectMapReduce;
import dht.LongDHT;
import octojus.ComputationRequest;
import octojus.OctojusNode;
import toools.math.DistributionForLongs;

import com.carrotsearch.hppc.cursors.LongCursor;

public final class DistanceDistribution extends BigObjectMapReduce<LongDHT, DistributionForLongs, DistributionForLongs>
{

	public DistanceDistribution(LongDHT g)
	{
		super(g);
	}

	private static class GetCCs extends BigObjectComputingRequest<LongDHT, DistributionForLongs>
	{

		public GetCCs(String id)
		{
			super(id);
		}

		@Override
		protected DistributionForLongs localComputation(LongDHT a)
		{
			DistributionForLongs distanceDistribution = new DistributionForLongs();

			for (LongCursor c : a.getLocalMap().values())
			{
				distanceDistribution.addOccurence(c.value);
			}

			return distanceDistribution;
		}
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

	@Override
	protected ComputationRequest<DistributionForLongs> map(OctojusNode n, LongDHT bo)
	{
		return new GetCCs(bo.getID());
	}

}