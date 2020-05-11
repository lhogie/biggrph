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

package biggrph.algo.connected_components;

import java.util.Map;

import bigobject.BigObjectComputingRequest;
import bigobject.BigObjectMapReduce;
import dht.set.HashLongSet64;
import dht.set.LongSet;
import dht.set.LongSets;
import octojus.ComputationRequest;
import octojus.OctojusNode;

import com.carrotsearch.hppc.cursors.LongCursor;

public final class EnumeratingConnectedComponents extends BigObjectMapReduce<AssignConnectedComponents, LongSet, LongSet>
{
	public EnumeratingConnectedComponents(AssignConnectedComponents cc)
	{
		super(cc);
	}

	@Override
	protected ComputationRequest<LongSet> map(OctojusNode node, AssignConnectedComponents bo)
	{
		return new GetCCs(bo.getID());
	}

	@Override
	protected LongSet reduce(Map<OctojusNode, LongSet> nodeResults)
	{
		return LongSets.union(nodeResults.values());
	}

	@SuppressWarnings("serial")
	private static class GetCCs extends BigObjectComputingRequest<AssignConnectedComponents, LongSet>
	{

		public GetCCs(String bigObjectID)
		{
			super(bigObjectID);
		}

		@Override
		protected LongSet localComputation(AssignConnectedComponents cc)
		{
			LongSet r = new HashLongSet64();

			for (LongCursor c : cc.getConnectedComponentsAssignments().getLocalMap().values())
			{
				r.add(c.value);
			}

			return r;
		}
	}
}