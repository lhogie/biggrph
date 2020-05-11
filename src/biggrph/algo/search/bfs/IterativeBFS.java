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

package biggrph.algo.search.bfs;

import biggrph.BigAdjacencyTable;
import toools.collection.bigstuff.longset.LongCursor;
import toools.util.LongPredicate;

public class IterativeBFS
{

	public static void compute(BigAdjacencyTable g, long src,
			IterativeBFSResult baseResult)
	{
		compute(g, src, baseResult, LongPredicate.ACCEPT_ALL);
	}

	public static void compute(BigAdjacencyTable g, long src,
			IterativeBFSResult baseResult, LongPredicate listener)
	{
		if ( ! g.isLocalElement(src))
			throw new IllegalArgumentException("vertex " + src + " is not local");

		baseResult.distances.put(src, 0);

		long[] queue = new long[(int) g.getNumberOfVertices()];
		int lowerBound = 0, upperBound = 0;
		queue[upperBound++] = src;
		long nbRound = 0;

		while (lowerBound != upperBound)
		{
			++nbRound;
			long v = queue[lowerBound++];
			long d = baseResult.distances.get(v);

			if (baseResult.visitOrder != null)
			{
				baseResult.visitOrder.add(v);
			}

			// if ( ! listener.continueAfterVisiting(v, d))
			// {
			// return;
			// }

			for (LongCursor c : g.getLocalData().get(v))
			{
				long neighbor = c.value;

				// if this vertex was not yet visited
				if (listener.accept(neighbor)
						&& ! baseResult.distances.containsKey(neighbor))
				{
					baseResult.distances.put(neighbor, d + 1);

					if (baseResult.predecessors != null)
					{
						baseResult.predecessors.put(neighbor, v);
					}

					queue[upperBound++] = neighbor;
				}
			}
		}

		baseResult.nbRound = nbRound;
	}
}
