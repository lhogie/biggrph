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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Random;

import octojus.ComputationRequest;
import octojus.NoReturn;
import octojus.OctojusNode;
import octojus.OneNodeOneRequest;
import toools.math.MathsUtilities;
import biggrph.BigAdjacencyTable;
import biggrph.BigGrphCluster;
import bigobject.BigObjectCluster;
import bigobject.BigObjectComputingRequest;
import dht.allocation.BasicHashDHTAllocation;

public class RandomGraph
{
	public static void createIncidences(final BigAdjacencyTable g, final long size,
			final int minDegree, final int maxDegree, final Random prng)
	{
		new OneNodeOneRequest<NoReturn>()
		{

			@Override
			protected ComputationRequest<NoReturn> createComputationRequestForNode(
					OctojusNode n)
			{
				LocalGeneration j = new LocalGeneration(g.getID());
				j.minDegree = minDegree;
				j.maxDegree = maxDegree;
				j.size = size;
				j.prng = prng;
				return j;
			}
		}.execute(g.getAllocation().getNodes());
	}

	@SuppressWarnings("serial")
	public static class LocalGeneration extends
			BigObjectComputingRequest<BigAdjacencyTable, NoReturn>
	{
		int minDegree;
		int maxDegree;
		long size;
		Random prng;

		public LocalGeneration(String graphID)
		{
			super(graphID);
		}

		@Override
		protected NoReturn localComputation(BigAdjacencyTable g)
		{
			// override the prng that came by serialization and generate the
			// same sequence on different nodes
			prng = new Random();
			long onePercent = size / 100;

			for (long v = 0; v < size; ++v)
			{
				if (onePercent > 0 && v % onePercent == 0)
					System.out.println((v / onePercent) + " %");

				if (g.isLocalElement(v))
				{
					int degree = MathsUtilities.pickRandomBetween(minDegree,
							maxDegree + 1, prng);

					for (int i = 0; i < degree; ++i)
					{
						long neighbor = MathsUtilities.pickLongBetween(0, size, prng);
						g.__local_add(v, neighbor);
					}
				}
			}

			return null;
		}
	}

	public static void main(String[] args) throws UnknownHostException, IOException
	{
		BigObjectCluster c = BigGrphCluster.workstations("k5000", "tina");
		BasicHashDHTAllocation allocation = new BasicHashDHTAllocation(c);

		BigAdjacencyTable g = new BigAdjacencyTable("test", allocation, 0, false);
		System.out.println("generating topology");
		createIncidences(g, 1000, 2, 100, new Random());
		System.out.println("completed");
		System.out.println(g.__numberOfVerticesPerNode());
		System.out.println(g.__getEdgeLocality());
	}
}
