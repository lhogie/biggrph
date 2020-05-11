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

import octojus.OctojusCluster;
import toools.io.TextNumberReader;
import toools.io.file.RegularFile;
import toools.math.MathsUtilities;
import toools.text.TextUtilities;
import biggrph.BigAdjacencyTable;
import biggrph.GraphTopology;
import dht.DHTAllocation;
import dht.allocation.BasicHashDHTAllocation;
import dht.set.ArrayLongSet64;
import dht.set.ArrayLongSet32;
import dht.set.LongSet;

public class AdjDataSet extends BigGrphDataSet
{
	public AdjDataSet(String fileName, OctojusCluster c)
	{
		this(fileName, new BasicHashDHTAllocation(c));
	}

	public AdjDataSet(RegularFile file, OctojusCluster cluster)
	{
		this(file, new BasicHashDHTAllocation(cluster));
	}

	public AdjDataSet(RegularFile file, DHTAllocation alloc)
	{
		this(file.getName(), alloc, file);
	}

	protected AdjDataSet(String id, DHTAllocation dp)
	{
		super(id, dp);
	}

	protected AdjDataSet(String id, DHTAllocation dp, Object... extraParameters)
	{
		super(id, dp, extraParameters);
	}

	protected AdjDataSet(String id, DHTAllocation dp, RegularFile file)
	{
		super(id, dp, file);
		setFile(file);
	}

	@Override
	public void loadLocalPartTo(GraphTopology topoloy) throws IOException
	{
		BigAdjacencyTable outAdjacencyTable = topoloy.getOutAdjacencyTable();
		boolean biDirectional = topoloy.isBiDirectional();
		boolean undirected = topoloy.isUndirected();
		BigAdjacencyTable inAdjacencyTable = topoloy.getInAdjacencyTable();
		long numberOfEdgesParsed = 0;
		long numberOfEdgesAdded = 0;
		InputStream is = ((RegularFile) getFile()).createReadingStream();
		TextNumberReader s = new TextNumberReader(is, 65536 * 256);

		while (s.hasNext())
		{
			long src = s.nextLong();
			long nbNeighbor = s.nextLong();
			final boolean srcIsLocal = outAdjacencyTable.isLocalElement(src);

			long[] array = srcIsLocal ? new long[(int) nbNeighbor] : null;

			if (biDirectional && inAdjacencyTable.isLocalElement(src))
			{
				inAdjacencyTable.__local_ensureExists(src);
			}

			for (int i = 0; i < nbNeighbor; ++i)
			{
				if (numberOfEdgesParsed > 0
						&& numberOfEdgesParsed % (20 * 1000 * 1000) == 0)
				{
					System.out.println(TextUtilities.toHumanString(numberOfEdgesParsed)
							+ " edges parsed");
				}

				long neighbor = s.nextLong();
				numberOfEdgesParsed++;

				if (srcIsLocal)
				{
					array[i] = neighbor;
					numberOfEdgesAdded++;
				}

				if (outAdjacencyTable.isLocalElement(neighbor))
				{
					outAdjacencyTable.__local_ensureExists(neighbor);
				}

				if (biDirectional && inAdjacencyTable.isLocalElement(neighbor))
				{
					inAdjacencyTable.__local_add(neighbor, src);
				}

				if (undirected && outAdjacencyTable.isLocalElement(neighbor))
				{
					outAdjacencyTable.__local_add(neighbor, src);
				}
			}

			if (srcIsLocal)
			{
				outAdjacencyTable.getLocalData().put(src, createAppropriateSet(array));
			}
		}

		System.out.println("Load completed: " + numberOfEdgesAdded + " edges added");
	}

	private LongSet createAppropriateSet(long[] array)
	{
		if (array.length == 0 || MathsUtilities.max(array) <= Integer.MAX_VALUE)
		{
			return new ArrayLongSet32(array);
		}
		else
		{
			return new ArrayLongSet64(array);
		}
	}
}
