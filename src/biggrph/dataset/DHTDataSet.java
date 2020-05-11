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

package biggrph.dataset;

import octojus.OctojusCluster;
import bigobject.Allocation;
import bigobject.DataSet;
import dht.DHT;
import dht.DHTAllocation;
import dht.allocation.BasicHashDHTAllocation;

public abstract class DHTDataSet<T extends DHT<?, ?>> extends DataSet<T>
{

	protected DHTDataSet(String id, Allocation alloc, Object... extraParameters)
	{
		super(id, alloc, extraParameters);
	}

	@Override
	protected DHTAllocation createDistributionPolicy(OctojusCluster cluster)
	{
		return new BasicHashDHTAllocation(cluster);

		// int nbOfGigsRequired = (int) Math.max(1, getFile().getSize() /
		// 1000000000);
		// return new RAMBasedHashDHTAllocation(nodes, nbOfGigsRequired);
	}

	@Override
	public DHTAllocation getAllocation()
	{
		return (DHTAllocation) super.getAllocation();
	}
}
