package biggrph._algo.count.nb_2cycle;

import java.util.Iterator;
import java.util.List;

import octojus.OctojusNode;
import toools.collection.bigstuff.longset.LongCursor;
import biggrph.BigAdjacencyTable;
import bigobject.BigObjectComputingRequest;


public class EstimatingNbOf2Cycles
{
	@SuppressWarnings("serial")
	private static class LocalCode extends BigObjectComputingRequest<BigAdjacencyTable, Long>
	{
		final int reductionFactor;

		public LocalCode(String bigObjectID, int reduction)
		{
			super(bigObjectID);
			this.reductionFactor = reduction;
		}

		@Override
		protected Long localComputation(BigAdjacencyTable adj) throws Throwable
		{
			long count = 0;
			int nbVerticesToConsider = adj.getLocalData().size() / reductionFactor;

			int nbVerticesConsidered = 0;
			Iterator<LongCursor> iterator = adj.getLocalVertices2().iterator();

			while (nbVerticesConsidered < nbVerticesToConsider && iterator.hasNext())
			{
				LongCursor v = iterator.next();

				for (LongCursor neighbor : adj.get(v.value))
				{
					if (adj.getAllocation().isLocalElement(neighbor.value))
					{
						if (adj.get(neighbor.value).contains(v.value))
						{
							++count;
						}
					}
				}

				++nbVerticesConsidered;
			}

			return count;
		}
	}

	public static long compute(BigAdjacencyTable g) throws Throwable
	{
		int reductionFactor = 1 + (int) (g.getLocalData().size() / 1000);
		return compute(g, reductionFactor);
	}

	public static long compute(BigAdjacencyTable g, int reductionFactor) throws Throwable
	{
		List<OctojusNode> nodes = g.getAllocation().getNodes();
		OctojusNode randomNode = nodes.iterator().next();

		return new LocalCode(g.getID(), reductionFactor).runOn(randomNode) * nodes.size()
				* nodes.size() * reductionFactor;
	}
}
