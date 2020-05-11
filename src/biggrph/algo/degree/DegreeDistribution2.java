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

import octojus.OctojusNode;
import toools.math.DistributionForLongs;
import biggrph.BigAdjacencyTable;
import bigobject.BigObjectMapReduce2;

import com.carrotsearch.hppc.cursors.LongCursor;

import dht.DHTAllocation;

public final class DegreeDistribution2 extends BigObjectMapReduce2<BigAdjacencyTable, DistributionForLongs, DistributionForLongs>
{
	public DegreeDistribution2(BigAdjacencyTable g)
	{
		super(g.getID() + "/degreeDistribution", g.getAllocation(), g.getID());
	}

	public DegreeDistribution2(String id, DHTAllocation segments, String graphID)
	{
		super(id, segments, graphID);
	}

	@Override
	protected DistributionForLongs map(OctojusNode n, BigAdjacencyTable g)
	{
		DistributionForLongs r = new DistributionForLongs();

		for (LongCursor c : g.getLocalData().keys())
		{
			long degree = g.getLocalData().get(c.value).size();
			r.addOccurence(degree);
		}

		return r;
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

}