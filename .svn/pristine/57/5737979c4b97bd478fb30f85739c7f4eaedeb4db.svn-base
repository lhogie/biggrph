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

package biggrph._algo.locality;

import java.util.Map;

import octojus.ComputationRequest;
import octojus.OctojusNode;
import octojus.programming_models.map_reduce.OctojusMapReduce;
import toools.StopWatch;
import toools.StopWatch.UNIT;
import toools.collection.bigstuff.longset.LongCursor;
import biggrph.AlgoResult;
import biggrph.BigAdjacencyTable;
import bigobject.BigObjectComputingRequest;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import dht.set.LongSet;

public class ActualEdgeLocalityAlgorithm extends
		OctojusMapReduce<EdgeLocalityInfo, EdgeLocalityInfo>
{
	public static AlgoResult<EdgeLocalityInfo> computeAndProfile(BigAdjacencyTable g)
	{
		StopWatch sw = new StopWatch(UNIT.ms);
		AlgoResult<EdgeLocalityInfo> r = new AlgoResult<EdgeLocalityInfo>();
		r.value = compute(g);
		r.computationDurationMs = sw.getElapsedTime();
		return r;
	}

	public static EdgeLocalityInfo compute(BigAdjacencyTable g)
	{
		return new ActualEdgeLocalityAlgorithm(g).execute(g.getAllocation().getNodes());
	}

	private final BigAdjacencyTable g;

	public ActualEdgeLocalityAlgorithm(BigAdjacencyTable g)
	{
		this.g = g;
	}

	@Override
	protected ComputationRequest<EdgeLocalityInfo> map(OctojusNode node)
	{
		return new LocalCode(g.getID());
	}

	@SuppressWarnings("serial")
	public static class LocalCode extends
			BigObjectComputingRequest<BigAdjacencyTable, EdgeLocalityInfo>
	{
		public LocalCode(String id)
		{
			super(id);
		}

		@Override
		protected EdgeLocalityInfo localComputation(BigAdjacencyTable g)
		{
			EdgeLocalityInfo r = new EdgeLocalityInfo();

			for (ObjectCursor<LongSet> c : g.getLocalData().values())
			{
				LongSet edges = c.value;
				r.totalNumberOfEdges += edges.size();

				for (LongCursor cc : edges)
				{
					if (g.isLocalElement(cc.value))
					{
						++r.nbOfLocalEdges;
					}
				}
			}

			return r;
		}
	};

	@Override
	protected EdgeLocalityInfo reduce(Map<OctojusNode, EdgeLocalityInfo> nodeResults)
	{
		EdgeLocalityInfo r = new EdgeLocalityInfo();

		for (EdgeLocalityInfo i : nodeResults.values())
		{
			r.nbOfLocalEdges += i.nbOfLocalEdges;
			r.totalNumberOfEdges += i.totalNumberOfEdges;
		}

		return r;
	}
}
