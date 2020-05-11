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

package biggrph.algo.randomVertex;

import java.util.Map;
import java.util.Random;

import octojus.ComputationRequest;
import octojus.OctojusNode;
import biggrph.BigAdjacencyTable;
import biggrph.GraphTopology;
import bigobject.BigObjectComputingRequest;
import bigobject.BigObjectMapReduce;

public class PickRandomVertices
		extends
		BigObjectMapReduce<GraphTopology, PickRandomVerticesLocalResult, PickRandomVerticesGlobalResult>
{
	private Random prng;
	private int nbVertices;

	public PickRandomVertices(GraphTopology topology, Random random, int n)
	{
		super(topology);
		this.nbVertices = n;
		this.prng = random;
	}

	@Override
	protected ComputationRequest<PickRandomVerticesLocalResult> map(OctojusNode n,
			GraphTopology g)
	{
		LocalCode lc = new LocalCode(g.getID());
		lc.nbVertices = this.nbVertices;
		lc.prng = this.prng;
		return lc;
	}

	@Override
	protected PickRandomVerticesGlobalResult reduce(
			Map<OctojusNode, PickRandomVerticesLocalResult> nodeResults)
	{
		PickRandomVerticesGlobalResult r = new PickRandomVerticesGlobalResult();
		r.nodeResults = nodeResults;
		return r;
	}

	@SuppressWarnings("serial")
	private static class LocalCode extends
			BigObjectComputingRequest<GraphTopology, PickRandomVerticesLocalResult>
	{
		private Random prng;
		private int nbVertices;

		public LocalCode(String id)
		{
			super(id);
		}

		@Override
		protected PickRandomVerticesLocalResult localComputation(GraphTopology topology)
		{
			PickRandomVerticesLocalResult lr = new PickRandomVerticesLocalResult();
			BigAdjacencyTable adj = topology.getOutAdjacencyTable();
			lr.numberOfLocalVertices = adj.getLocalData().size();
			lr.randomVertices.addAll(adj.getLocalData().pickRandomKeys(nbVertices, prng));
			return lr;
		}
	}

	public static PickRandomVerticesGlobalResult compute(GraphTopology topology, int n,
			Random random)
	{
		return new PickRandomVertices(topology, random, n).execute();
	}
}
