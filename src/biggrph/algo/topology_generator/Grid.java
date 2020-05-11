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

package biggrph.algo.topology_generator;

import java.util.Map;

import biggrph.BigAdjacencyTable;
import bigobject.BigObjectComputingRequest;
import bigobject.BigObjectMapReduce;
import octojus.ComputationRequest;
import octojus.NoReturn;
import octojus.OctojusNode;

public class Grid
{
	public static void grid(final BigAdjacencyTable g, final long width,
			final long height, final boolean directed, final boolean diags)
	{
		new BigObjectMapReduce<BigAdjacencyTable, NoReturn, Object>(g)
		{

			@Override
			protected ComputationRequest<NoReturn> map(OctojusNode n, BigAdjacencyTable bo)
			{
				LocalGeneration j = new LocalGeneration(g.getID());
				j.width = width;
				j.height = height;
				j.diags = diags;
				j.directed = directed;
				return j;
			}

			@Override
			protected NoReturn reduce(Map<OctojusNode, NoReturn> r)
			{
				return null;
			}
		}.execute();

	}

	@SuppressWarnings("serial")
	static class LocalGeneration extends
			BigObjectComputingRequest<BigAdjacencyTable, NoReturn>
	{
		private long width, height = - 1;
		private boolean diags;
		private boolean directed;

		public LocalGeneration(String graphID)
		{
			super(graphID);
		}

		@Override
		protected NoReturn localComputation(BigAdjacencyTable g)
		{
			for (int i = 0; i < width; ++i)
			{
				for (int j = 0; j < height; ++j)
				{
					addEdgeIfInRange(g, i, j, i + 1, j);
					addEdgeIfInRange(g, i, j, i, j + 1);

					if (diags)
					{
						addEdgeIfInRange(g, i, j, i + 1, j + 1);
					}

					if ( ! directed)
					{
						addEdgeIfInRange(g, i + 1, j, i, j);
						addEdgeIfInRange(g, i, j + 1, i, j);
					}
				}
			}

			return null;
		}

		private void addEdgeIfInRange(BigAdjacencyTable g, long i1, long j1, long i2,
				long j2)
		{
			if (i1 < width && j1 < height && j1 >= 0 && i2 < width && j2 < height
					&& j2 >= 0)
			{
				long v = i1 + width * j1;
				long neighbor = i2 + width * j2;

				if (g.isLocalElement(v))
				{
					g.__local_add(v, neighbor);
				}

				if (g.isLocalElement(neighbor))
				{
					g.__local_ensureExists(neighbor);
				}
			}
		}
	}
}
