package biggrph.algo.search.bfs;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.cursors.LongCursor;

import biggrph.Path;
import toools.collections.Arrays;

public class IterativeBFSResult
{
	public final LongLongMap distances;
	public final LongLongMap predecessors;
	public final LongArrayList visitOrder;
	public final long source;
	public long nbRound;

	public IterativeBFSResult(long src, LongLongMap distances,
			LongLongMap predecessors, LongArrayList visitOrder)
	{
		this.source = src;
		this.distances = distances;
		this.predecessors = predecessors;
		this.visitOrder = visitOrder;
	}

	public long farestVertex()
	{
		if (visitOrder != null)
		{
			return visitOrder.get(visitOrder.size() - 1);
		}
		else
		{
			long v = - 1;
			long dv = - 1;

			for (LongCursor c : distances.keys())
			{
				long d = distances.get(c.value);

				if (d > dv)
				{
					dv = d;
					v = c.value;
				}
			}

			return v;
		}
	}

	public long maxDistance()
	{
		return distances.get(farestVertex());
	}

	public Path computePathTo(long v)
	{
		if (predecessors == null)
			throw new IllegalStateException(
					"predecessor map was not computed. You can't have a path");

		long[] a = new long[(int) distances.get(v) + 1];

		for (int i = 0; i < a.length; ++i, v = predecessors.get(v))
		{
			a[i] = v;
		}

		Arrays.reverse(a);
		return new Path(a);
	}

	@Override
	public String toString()
	{
		return "" + maxDistance();
	}
}