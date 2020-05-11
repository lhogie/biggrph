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

package biggrph.algo.diameter.ifub;

import biggrph.GraphTopology;
import bigobject.BigObjectComputingRequest;
import bigobject.BigObjectRegistry;

import com.carrotsearch.hppc.cursors.LongCursor;

import dht.LongDHT;
import toools.util.LongPredicate;

@SuppressWarnings("serial")
class iFUBComputationRequest
		extends BigObjectComputingRequest<GraphTopology, iFUBCompResult>
{
	long layer;
	String distancesDHTid;
	LongPredicate vertexMatcher;

	public iFUBComputationRequest(String id)
	{
		super(id);
	}

	@Override
	protected iFUBCompResult localComputation(GraphTopology gTopology)
	{
		LongDHT localmapD = (LongDHT) BigObjectRegistry.defaultRegistry
				.get(distancesDHTid);
		long lb = - 1;
		long nbBFS = 0;

		for (LongCursor vertex : localmapD.getLocalData().keys())
		{
			assert localmapD.getLocalData().containsKey(vertex.value);

			if (localmapD.getLocalData().get(vertex.value) == layer)
			{
				nbBFS++;
				System.out.println("computeFarthestVertex(" + vertex.value);
				long ve = gTopology.getOutAdjacencyTable()
						.computeFarthestVertex(vertex.value, vertexMatcher).getValue();

				if (ve > lb)
				{
					lb = ve;
				}
			}
		}

		iFUBCompResult r = new iFUBCompResult();
		r.maxDistanceFound = lb;
		r.nBFS = nbBFS;
		return r;
	}

}