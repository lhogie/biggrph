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

package biggrph.algo.degree;

import jacaboo.LucCluster;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Random;

import biggrph.BigAdjacencyTable;
import biggrph.BigGrphCluster;
import biggrph.algo.topology_generator.RandomGraph;
import bigobject.BigObjectCluster;
import dht.allocation.BasicHashDHTAllocation;

public class MaxDegreeDemo
{
	public static void main(String[] args) throws UnknownHostException, IOException
	{
		BigObjectCluster a = BigGrphCluster.workstations("lhogie", new LucCluster(), 1);

		BasicHashDHTAllocation allocation = new BasicHashDHTAllocation(a);
		System.out.println(allocation);

		BigAdjacencyTable g = new BigAdjacencyTable("test", allocation, 0, false);

		System.out.println("generating topology");
		RandomGraph.createIncidences(g, 1000, 2, 10, new Random());

		System.out.println("computing max degree");
		long maxDegree = g.getMaxDegree().degree;
		System.out.println(maxDegree);
	}
}
