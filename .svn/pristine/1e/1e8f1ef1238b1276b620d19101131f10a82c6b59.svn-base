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

package biggrph.algo.diameter;

import java.util.ArrayList;
import java.util.List;

import biggrph.GraphTopology;
import dht.KeyValuePair;
import toools.util.LongPredicate;

public final class NBFSDiameter
{
	public static class NBFSDiameterResult
	{
		public final long startVertex;
		public final List<KeyValuePair<Long>> l = new ArrayList<KeyValuePair<Long>>();
		
		public NBFSDiameterResult(long sv)
		{
			this.startVertex = sv;
		}
		
		public long getDiameter()
		{
			return l.get(l.size() - 1).getValue();
		}
		
		public long getNumberOfBFS()
		{
			return l.size();
		}
	}


	public static NBFSDiameterResult computeDiameterIncrements(GraphTopology gTopology, LongPredicate vertexMatcher)
	{
		long src = gTopology.getOutMaxDegree().vertex;
		return computeDiameterIncrements(gTopology, src, vertexMatcher);
	}

	public static NBFSDiameterResult computeDiameterIncrements(GraphTopology gTopology, long startVertex, LongPredicate vertexMatcher)
	{
		NBFSDiameterResult r = new NBFSDiameterResult(startVertex);

		while (true)
		{
			KeyValuePair<Long> f = gTopology.getOutAdjacencyTable().computeFarthestVertex(startVertex, vertexMatcher);

			if (r.l.isEmpty() || f.getValue() > r.l.get(r.l.size() - 1).getValue() )
			{
				r.l.add(f);
				startVertex = f.key;
			}
			else
			{
				// the new distance isn't better, do not iterating again
				return r;
			}
		}
	}
}