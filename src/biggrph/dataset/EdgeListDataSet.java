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

import java.io.IOException;
import java.io.InputStream;

import biggrph.BigAdjacencyTable;
import biggrph.GraphTopology;
import dht.DHTAllocation;
import dht.allocation.BasicHashDHTAllocation;
import dht.set.LongSet;
import octojus.OctojusCluster;
import toools.StopWatch;
import toools.io.TextNumberReader;
import toools.io.file.RegularFile;
import toools.text.TextUtilities;

/**
 * An EdgeListDataSet represent a graph data stored in a edge list file, that is
 * a set of edges as a couple of two long integers. The first number is the
 * source and the second is the destination of the edge.<br>
 * 
 * Graph Datasets are created either by specifying a name as a String object, or
 * by specifying a file with a {@link RegularFile} object. A string argument to
 * the constructor is supposed to be the name of a file containing the graph
 * data and located inside the application dataset directory. A
 * {@link RegularFile} argument is supposed to specify the full path of the
 * graph data file, without any interaction with the dataset directory.
 * 
 * @author Luc Hogie
 * @author Nicolas Chleq
 */
public class EdgeListDataSet extends BigGrphDataSet
{
	/**
	 * Creates and returns an EdgeListDataSet whose name is datasetName. This
	 * name is supposed to be the name of a edge list file located inside the
	 * application dataset directory.
	 * 
	 * @param datasetName
	 * @param cluster
	 */
	public EdgeListDataSet(String datasetName, OctojusCluster cluster)
	{
		this(datasetName, new BasicHashDHTAllocation(cluster));
	}

	public EdgeListDataSet(RegularFile file, OctojusCluster cluster)
	{
		this(file, new BasicHashDHTAllocation(cluster));
	}

	public EdgeListDataSet(RegularFile file, DHTAllocation alloc)
	{
		this(file.getName(), alloc, file);
	}

	protected EdgeListDataSet(String datasetName, DHTAllocation dp)
	{
		super(datasetName, dp);
	}

	protected EdgeListDataSet(String datasetName, DHTAllocation dp, RegularFile file)
	{
		super(datasetName, dp, file);
		setFile(file);
	}

	@Override
	public void loadLocalPartTo(GraphTopology topology) throws IOException
	{
		BigAdjacencyTable outAdjTable = topology.getOutAdjacencyTable();
		BigAdjacencyTable inAdjTable = topology.getInAdjacencyTable();
		boolean biDirectional = topology.isBiDirectional();
		boolean undirected = topology.isUndirected();

		InputStream is = getFile().createReadingStream();
		TextNumberReader s = new TextNumberReader(is, 65536 * 256);
		int numberOfEdgesParsed = 0;
		int numberOfEdgesAdded = 0;
		int numberOfIgnoredEdges = 0;
		int numberOfRevEdgesAdded = 0;
		int numberOfRevIgnoredEdges = 0;
		@SuppressWarnings("unused")
		StopWatch sw = new StopWatch();
		long previousSrc = - 1;
		long previousDst = - 1;

		while (s.hasNext())
		{
			if (numberOfEdgesParsed > 0 && numberOfEdgesParsed % (10 * 1000 * 1000) == 0)
			{
				System.out.println(TextUtilities.toHumanString(numberOfEdgesParsed)
						+ " edges parsed");
			}

			numberOfEdgesParsed++;

			// reads the vertex IDs
			long srcId = s.nextLong();
			long dstId = s.nextLong();
			// Edge is from srcId to dstId: add it to the outAjacencyTable and
			// if the topology
			// is bidirectional add dstId -> srcId to the inAdjacencyTable.
			if (outAdjTable.isLocalElement(dstId))
			{
				outAdjTable.__local_ensureExists(dstId);
			}

			if (biDirectional && inAdjTable.isLocalElement(srcId))
			{
				inAdjTable.__local_ensureExists(srcId);
			}
			if (undirected && outAdjTable.isLocalElement(srcId))
			{
				outAdjTable.__local_ensureExists(srcId);
			}
			// previousSrc acts as a cache of the result of the call to
			// isLocalElement()
			if (srcId == previousSrc || outAdjTable.isLocalElement(srcId))
			{
				LongSet adjList = outAdjTable.getLocalMap().get(srcId);
				int sizeBefore = (adjList == null ? 0 : adjList.size());
				outAdjTable.__local_add(srcId, dstId);
				adjList = outAdjTable.getLocalMap().get(srcId);
				int sizeAfter = (adjList == null ? 0 : adjList.size());
				if (sizeAfter > sizeBefore)
				{
					numberOfEdgesAdded++;
					if (numberOfEdgesAdded > 0
							&& numberOfEdgesAdded % (10 * 1000 * 1000) == 0)
					{
						System.out.println(TextUtilities.toHumanString(numberOfEdgesAdded)
								+ " edges added");
					}
				}
				else
				{
					numberOfIgnoredEdges++;
				}
				previousSrc = srcId;
			}

			if (biDirectional
					&& (dstId == previousDst || inAdjTable.isLocalElement(dstId)))
			{
				LongSet adjList = inAdjTable.getLocalMap().get(srcId);
				int sizeBefore = (adjList == null ? 0 : adjList.size());
				inAdjTable.__local_add(dstId, srcId);
				adjList = inAdjTable.getLocalMap().get(srcId);
				int sizeAfter = (adjList == null ? 0 : adjList.size());
				if (sizeAfter > sizeBefore)
				{
					numberOfRevEdgesAdded++;
				}
				else
				{
					numberOfRevIgnoredEdges++;
				}
				previousDst = dstId;
			}
			if (undirected && outAdjTable.isLocalElement(dstId))
			{
				LongSet adjList = outAdjTable.getLocalMap().get(dstId);
				int sizeBefore = (adjList == null ? 0 : adjList.size());
				outAdjTable.__local_add(dstId, srcId);
				adjList = outAdjTable.getLocalMap().get(dstId);
				int sizeAfter = (adjList == null ? 0 : adjList.size());
				if (sizeAfter > sizeBefore)
				{
					numberOfEdgesAdded++;
					if (numberOfEdgesAdded > 0
							&& numberOfEdgesAdded % (10 * 1000 * 1000) == 0)
					{
						System.out.println(TextUtilities.toHumanString(numberOfEdgesAdded)
								+ " edges added");
					}
				}
				else
				{
					numberOfIgnoredEdges++;
				}
				previousDst = dstId;
			}
		}

		System.out.println("Load completed: " + numberOfEdgesAdded + " edges added, "
				+ numberOfIgnoredEdges + " edges ignored.");
		if (biDirectional)
		{
			System.out.println("   " + numberOfRevEdgesAdded + " reverse edges added, "
					+ numberOfRevIgnoredEdges + " reverse edges ignored.");
		}

		is.close();
	}

	public long getOneVertex() throws IOException
	{
		InputStream is = getFile().createReadingStream();
		TextNumberReader s = new TextNumberReader(is, 1024);
		return s.nextLong();
	}
}
