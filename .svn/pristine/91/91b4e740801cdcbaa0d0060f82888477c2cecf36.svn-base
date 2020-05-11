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

import java.util.ArrayList;
import java.util.List;

import biggrph.GraphTopology;
import biggrph.algo.search.bfs.BFS.BFSResult;
import octojus.OctojusNode;
import toools.StopWatch;
import toools.util.LongPredicate;

public final class IFub
{
	public static List<IfubListener> listeners = new ArrayList<IfubListener>();

	public static IFUBResult computeDiameter(GraphTopology gTopology, long src,
			LongPredicate vertexMatcher)
	{

		StopWatch sw = new StopWatch();
		BFSResult bfsResult = gTopology.getOutAdjacencyTable().bfs(src, false,
				vertexMatcher);
		long lowerb = bfsResult.distanceDHT.maxValue(null).getValue();
		long nBFS = 0;

		for (long layer = lowerb; layer > 0; layer--)
		{
			for (IfubListener l : listeners)
			{
				l.layer(layer);
			}

			for (OctojusNode node : gTopology.getAllocation().getNodes())
			{
				iFUBComputationRequest cr = new iFUBComputationRequest(gTopology.getID());
				cr.layer = layer;
				cr.distancesDHTid = bfsResult.distanceDHT.getID();
				cr.vertexMatcher = vertexMatcher;

				try
				{
					// System.out.println("####### running on " + node);
					iFUBCompResult r = cr.runOn(node);
					long newLowerBound = r.maxDistanceFound;
					nBFS += r.nBFS;

					if (newLowerBound > lowerb)
					{
						lowerb = newLowerBound;

						for (IfubListener l : listeners)
						{
							l.newLowerBound(lowerb);
						}
					}
				}
				catch (Throwable t)
				{
					throw new IllegalStateException(t);
				}
			}

			if (lowerb >= (2 * layer) - 1)
			{
				break;
			}
		}

		bfsResult.distanceDHT.delete();

		IFUBResult res = new IFUBResult();
		res.diameter = lowerb;
		res.durationMs = sw.getElapsedTime();
		res.nbBFS = nBFS;
		return res;
	}
}