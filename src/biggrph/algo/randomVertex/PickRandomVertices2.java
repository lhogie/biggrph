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

import octojus.OctojusNode;
import biggrph.GraphTopology;
import bigobject.BigObjectMapReduce2;
import dht.DHTAllocation;

public class PickRandomVertices2 extends
		BigObjectMapReduce2<GraphTopology, PickRandomVerticesLocalResult, PickRandomVerticesGlobalResult>
{
	private Random prng;
	private int nbVertices;

	public PickRandomVertices2(GraphTopology cc, int nbVertices, Random random)
	{
		this(cc.getID() + "/randomVertex", cc.getAllocation(), cc.getID(), nbVertices,
				random);
	}

	public PickRandomVertices2(String id, DHTAllocation allocation, String boID,
			int nbVertices, Random random)
	{
		super(id, allocation, boID, nbVertices, random);
		this.prng = random;
		this.nbVertices = nbVertices;
	}

	@Override
	protected PickRandomVerticesLocalResult map(OctojusNode n, GraphTopology topology)
	{
		PickRandomVerticesLocalResult lr = new PickRandomVerticesLocalResult();
		lr.numberOfLocalVertices = topology.getOutAdjacencyTable().getLocalData().size();
		lr.randomVertices.addAll(topology.getOutAdjacencyTable().getLocalData().pickRandomKeys(nbVertices, prng));
		return lr;
	}

	@Override
	protected PickRandomVerticesGlobalResult reduce(
			Map<OctojusNode, PickRandomVerticesLocalResult> nodeResults)
	{
		PickRandomVerticesGlobalResult r = new PickRandomVerticesGlobalResult();
		r.nodeResults = nodeResults;
		return r;
	}

	public static PickRandomVerticesGlobalResult compute(GraphTopology g, int n,
			Random random)
	{
		return new PickRandomVertices2(g, n, random).execute();
	}
}
