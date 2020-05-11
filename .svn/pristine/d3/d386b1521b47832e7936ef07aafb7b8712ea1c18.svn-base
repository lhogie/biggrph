package biggrph.algo.search.bfs;

import java.util.List;

import biggrph.BigAdjacencyTable;
import dht.LongDHT;
import dht.SerializableLongLongMap;
import ldjo.bsp.BSPComputation;
import ldjo.bsp.BSPListener;
import ldjo.bsp.SuperStepInfo;
import ldjo.bsp.msg.EmptyMessage;
import octojus.NoReturn;
import toools.thread.NThreadsPolicy;
import toools.util.LongPredicate;

public class BFS
{
	public static class BFSResult
	{
		public LongDHT distanceDHT;
		public LongDHT predecessorMap;
	}

	static SerializableLongLongMap distances;

	public static BFSResult compute(BigAdjacencyTable adj, long src,
			boolean computePredecessors, LongPredicate driver)
	{
		BFSResult rr = new BFSResult();

		if (adj.getAllocation().getNodes().size() == 1)
		{
			//if (distances == null)
				distances = new SerializableLongLongMap();

			SerializableLongLongMap predecessors = computePredecessors
					? new SerializableLongLongMap() : null;

			IterativeBFSResult bfsResult = new IterativeBFSResult(src, distances,
					predecessors, null);

			IterativeBFS.compute(adj, src, bfsResult, driver);

			rr.distanceDHT = new LongDHT(
					adj.getID() + "/bfs from " + src + "/distances", adj.getAllocation());
			rr.distanceDHT.setLocalData(distances);

			if (computePredecessors)
			{
				LongDHT predecessorDHT = new LongDHT(
						adj.getID() + "/bfs from " + src + "/predecessors",
						adj.getAllocation());
				predecessorDHT.setLocalData(predecessors);

				rr.predecessorMap = predecessorDHT;
			}
		}
		else
		{
			BSPListener<BigAdjacencyTable, EmptyMessage, NoReturn> listener = new BSPListener<BigAdjacencyTable, EmptyMessage, NoReturn>(){

				@Override
				public void computationStart(
						BSPComputation<BigAdjacencyTable, EmptyMessage, NoReturn> bsp)
				{
					// TODO Auto-generated method stub
					
				}

				@Override
				public void stepStarting(
						BSPComputation<BigAdjacencyTable, EmptyMessage, NoReturn> bsp,
						int superstep)
				{
					System.out.println(bsp);
				}

				@Override
				public void stepDone(
						BSPComputation<BigAdjacencyTable, EmptyMessage, NoReturn> bsp,
						List<SuperStepInfo<NoReturn>> superstepInfos)
				{
//					System.out.println("ghfhjdz, done");
				}

				@Override
				public void computationEnd(
						BSPComputation<BigAdjacencyTable, EmptyMessage, NoReturn> bsp)
				{
					// TODO Auto-generated method stub
					
				}};
			BSPBAsedBFS r = BSPBAsedBFS.compute(adj, src, listener,
					new NThreadsPolicy(1), driver);
			rr.distanceDHT = r.getDistancesDistributedMap();
			r.delete();
		}

		return rr;
	}
}
