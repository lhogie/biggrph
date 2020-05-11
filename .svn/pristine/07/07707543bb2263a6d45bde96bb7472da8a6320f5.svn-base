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
import biggrph.BigAdjacencyTable;
import bigobject.BigObjectComputingRequest;
import bigobject.BigObjectMapReduce;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import dht.set.LongSet;

/**
 * Computes the number of vertices and edges on every computer.
 * 
 * @author lhogie
 *
 */

public final class NumberOfVerticesAndEdgesPerNode extends BigObjectMapReduce<BigAdjacencyTable, EdgeAndVertexCount, Map<OctojusNode, EdgeAndVertexCount>>
{

	public NumberOfVerticesAndEdgesPerNode(BigAdjacencyTable g)
	{
		super(g);
	}

	@Override
	protected ComputationRequest<EdgeAndVertexCount> map(OctojusNode n, BigAdjacencyTable g)
	{
		return new NumberOfVerticesLocalExec(g.getID());
	}

	@Override
	protected Map<OctojusNode, EdgeAndVertexCount> reduce(Map<OctojusNode, EdgeAndVertexCount> r)
	{
		return r;
	}

	@SuppressWarnings("serial")
	public static class NumberOfVerticesLocalExec extends BigObjectComputingRequest<BigAdjacencyTable, EdgeAndVertexCount>
	{
		public NumberOfVerticesLocalExec(String id)
		{
			super(id);
		}

		@Override
		protected EdgeAndVertexCount localComputation(BigAdjacencyTable g)
		{
			EdgeAndVertexCount r = new EdgeAndVertexCount();
			r.nbVertices = g.getLocalData().keys().size();

			for (ObjectCursor<LongSet> c : g.getLocalData().values())
			{
				r.nbEdges += c.value.size();
			}

			return r;
		}
	}

	public static Map<OctojusNode, EdgeAndVertexCount> compute(BigAdjacencyTable g)
	{
		return new NumberOfVerticesAndEdgesPerNode(g).execute();
	}

}