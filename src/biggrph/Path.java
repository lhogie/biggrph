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

package biggrph;

import com.carrotsearch.hppc.LongArrayList;

import dht.LongDHT;
import toools.collections.Arrays;

public class Path
{
	public long[] array;

	public Path(long[] array)
	{
		if (array.length == 0)
			throw new IllegalArgumentException(
					"a path must have a least one vertex (which then would be both the source and the destination");

		this.array = array;
	}

	public int getSize()
	{
		return array.length - 1;
	}

	@Override
	public String toString()
	{
		StringBuilder b = new StringBuilder();

		for (int i = 0; i < array.length; ++i)
		{
			b.append(array[i]);

			if (i < array.length - 1)
			{
				b.append(" > ");
			}
		}

		return b.toString();
	}

	public static Path getShortestPathTo(long src, long dest, LongDHT predecessors)
	{
		LongArrayList s = new LongArrayList();

		long v = dest;

		while (v != src)
		{
			s.add(v);

			if (predecessors.containsKey(v))
			{
				v = predecessors.get(v);
			}
			else
			{
				throw new IllegalStateException(
						"path is not applicable. Vertex " + v + " has no predecessor");
			}
		}

		s.add(src);
		long[] vertexArray = s.toArray();
		Arrays.reverse(vertexArray);
		return new Path(vertexArray);
	}

	public long getCenterVertex()
	{
		return array[array.length / 2];
	}

}
