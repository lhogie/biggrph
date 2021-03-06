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

package biggrph._algo.count;

import java.util.Map;

import octojus.ComputationRequest;
import octojus.OctojusNode;
import toools.math.Distribution;
import biggrph.BigAdjacencyTable;
import bigobject.BigObjectComputingRequest;
import bigobject.BigObjectMapReduce;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import dht.set.LongSet;

/**
 * This algorithm computes a distribution of the class that is used in the adjacency table. 
 * 
 * 
 * @author lhogie
 *
 */

@SuppressWarnings("rawtypes")
public final class NeighborhoodContainerClassDistribution extends BigObjectMapReduce<BigAdjacencyTable, Distribution<Class>, Distribution<Class>>
{

	public NeighborhoodContainerClassDistribution(BigAdjacencyTable g)
	{
		super(g);
	}

	@Override
	protected ComputationRequest<Distribution<Class>> map(OctojusNode n, BigAdjacencyTable g)
	{
		return new NumberOfVerticesLocalExec(g.getID());
	}

	@Override
	protected Distribution<Class> reduce(Map<OctojusNode, Distribution<Class>> r)
	{
		Distribution<Class> d = new Distribution<Class>();

		for (Distribution<Class> s : r.values())
		{
			for (Class<?> cc : s.getOccuringObjects())
			{
				d.addNOccurences(cc, s.getNumberOfOccurences(cc));
			}
		}

		return d;
	}

	@SuppressWarnings({ "serial" })
	static class NumberOfVerticesLocalExec extends BigObjectComputingRequest<BigAdjacencyTable, Distribution<Class>>
	{
		public NumberOfVerticesLocalExec(String id)
		{
			super(id);
		}

		@Override
		protected Distribution<Class> localComputation(BigAdjacencyTable g)
		{
			Distribution<Class> r = new Distribution<Class>();

			for (ObjectCursor<LongSet> c : g.getLocalData().values())
			{
				r.addOccurence(c.value.getClass());
			}

			return r;
		}
	}

	public static Distribution<Class> compute(BigAdjacencyTable g)
	{
		return new NeighborhoodContainerClassDistribution(g).execute();
	}

}