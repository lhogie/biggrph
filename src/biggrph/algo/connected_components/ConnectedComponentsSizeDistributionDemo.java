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

import jacaboo.LucCluster;

import java.io.IOException;
import java.util.Random;

import bigobject.BigObjectCluster;
import dht.allocation.BasicHashDHTAllocation;
import toools.math.DistributionForLongs;
import biggrph.BigAdjacencyTable;
import biggrph.BigGrphCluster;
import biggrph.GraphTopology;
import biggrph.algo.topology_generator.RandomGraph;

public class ConnectedComponentsSizeDistributionDemo
{
	public static void main(String[] args) throws IOException
	{
		// start the communication infrastructure
		BigObjectCluster o = BigGrphCluster.workstations(System.getProperty("user.name"), new LucCluster(), 1);

		// define vertex repartition to cluster nodes
		BasicHashDHTAllocation allocation = new BasicHashDHTAllocation(o);

		// deploy graph
		GraphTopology gTopology = new GraphTopology("random-graph", allocation, false, false, 0);
		BigAdjacencyTable g = gTopology.getOutAdjacencyTable();

		// create random topology
		// Chain.chain(g);
		RandomGraph.createIncidences(g, 500, 0, 5, new Random());

		// run BSP page rank
		AssignConnectedComponents pr = new AssignConnectedComponents(gTopology);
		pr.execute();

		DistributionForLongs n = new ConnectedComponentsSize(pr).execute();
		System.out.println("distribution: " + n);
	}
}
