package biggrph.algo.topology_generator;

import octojus.ComputationRequest;
import octojus.NoReturn;
import octojus.OctojusNode;
import octojus.OneNodeOneRequest;
import toools.collection.bigstuff.longset.LongCursor;
import biggrph.BigAdjacencyTable;
import bigobject.BigObjectRegistry;


import dht.set.LongSet;

public class Undirectionalizer
{
	public static void indirect(BigAdjacencyTable g)
	{
		new OneNodeOneRequest<NoReturn>()
		{

			@Override
			protected ComputationRequest<NoReturn> createComputationRequestForNode(
					OctojusNode n)
			{
				IndirectTable a = new IndirectTable();
				a.gid = g.getID();
				return a;
			}
		}.execute(g.getAllocation().getNodes());
	}

	static class IndirectTable extends ComputationRequest<NoReturn>
	{
		String gid, reverseTableID;

		@Override
		protected NoReturn compute() throws Throwable
		{
			BigAdjacencyTable g = (BigAdjacencyTable) BigObjectRegistry.defaultRegistry
					.get(gid);

			for (LongCursor vc : LongCursor.fromHPPC(g.getLocalData().keys()))
			{
				long v = vc.value;
				LongSet neighbors = g.getLocalData().get(v);

				for (LongCursor nc : neighbors)
				{
					long neighbor = nc.value;
					g.add(neighbor, v);
				}
			}

			g.ensureRemoteAddsHaveCompleted();
			return null;
		}
	}
}
