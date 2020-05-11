package biggrph.algo.topology_generator;

import java.io.IOException;
import java.io.Serializable;

import octojus.ComputationRequest;
import octojus.NoReturn;
import octojus.OctojusNode;
import octojus.OneNodeOneRequest;
import toools.collection.bigstuff.longset.LongCursor;
import toools.io.FullDuplexDataConnection2;
import toools.net.TCPConnection;
import biggrph.BigAdjacencyTable;
import bigobject.Allocation;
import bigobject.BigObjectComputingRequest;
import bigobject.BigObjectRegistry;
import bigobject.LiveDistributedObject;
import bigobject.Service;

import com.carrotsearch.hppc.cursors.LongIntCursor;
import com.carrotsearch.hppc.cursors.LongObjectCursor;

import dht.DHTAllocation;
import dht.IntDHT;
import dht.set.LongSet;

public class ReverseNavigationEnabler
{
	public static BigAdjacencyTable createReverseTable(BigAdjacencyTable adjTable, String revAdjTableId)
	{
		return createReverseTable(adjTable, revAdjTableId, false);
	}
	
	public static BigAdjacencyTable createReverseTable(BigAdjacencyTable adjTable, String revAdjTableId, boolean verbose)
	{
		long nVertices = adjTable.getNumberOfVertices();
		BigAdjacencyTable reverseAdjTable = new BigAdjacencyTable(revAdjTableId, adjTable.getAllocation(), nVertices, adjTable.isMultigraph());
		IntDHT reverseSizes = new IntDHT(revAdjTableId + "/sizes", adjTable.getAllocation(), nVertices);
		
		ReverseSizeComputation sizeComputation = new ReverseSizeComputation(adjTable, reverseSizes, verbose);
		sizeComputation.execute();
		sizeComputation.delete();
		
		new OneNodeOneRequest<NoReturn>()
		{
			@Override
			protected ComputationRequest<NoReturn> createComputationRequestForNode(
					OctojusNode n)
			{
				ComputeLocalReverseTable job = new ComputeLocalReverseTable();
				job.gid = adjTable.getID();
				job.reverseTableID = reverseAdjTable.getID();
				job.reverseSizesId = reverseSizes.getID();
				job.verbose = verbose;
				return job;
			}
		}.execute(adjTable.getAllocation().getNodes());

		reverseSizes.delete();
		
		return reverseAdjTable;
	}

	@SuppressWarnings("serial")
	static class ComputeLocalReverseTable extends ComputationRequest<NoReturn>
	{
		String gid, reverseTableID, reverseSizesId;
		boolean verbose = false;

		@Override
		protected NoReturn compute() throws Throwable
		{
			BigAdjacencyTable graph = (BigAdjacencyTable) BigObjectRegistry.defaultRegistry
					.get(gid);
			BigAdjacencyTable reverseTable = (BigAdjacencyTable) BigObjectRegistry.defaultRegistry
					.get(reverseTableID);
			IntDHT reverseSizes = (IntDHT) BigObjectRegistry.defaultRegistry
					.get(reverseSizesId);

			if (verbose)
			{
				System.out.println("Allocating reverse adjacency table.");
			}
			
			for (LongIntCursor vertexCursor : reverseSizes.getLocalData())
			{
				long vertex = vertexCursor.key;
				int nNeighbors = vertexCursor.value;
				reverseTable.__local_ensureFit(vertex, true, nNeighbors);
			}
			
			if (verbose)
			{
				System.out.println("Filling reverse adjacency table.");
			}
			
			int nVertices = graph.getLocalMap().size();
			int progressStep = (nVertices / (10 * 10000)) * 10000;
			progressStep = Math.min( Math.max(progressStep, 100 * 1000), 100 * 1000 * 1000);
			int vertexCount = 0;
			int localEdgeCount = 0;
			int remoteEdgeCount = 0;
			for (LongObjectCursor<LongSet> vc : graph.getLocalData())
			{
				long src = vc.key;
				LongSet neighbors = vc.value;
				
				if (graph.getAllocation().isLocalElement(src))
				{
					reverseTable.__local_ensureFit(src, true, reverseSizes.get(src));
				}
				else
				{
					reverseTable.ensureExists(src);
				}

				for (LongCursor nc : neighbors)
				{
					long neighbor = nc.value;
					if (graph.getAllocation().isLocalElement(neighbor))
					{
						LongSet neighborSet = reverseTable.__local_ensureFit(neighbor, src >= Integer.MAX_VALUE,
								reverseSizes.get(neighbor));
						neighborSet.add(src);
						localEdgeCount++;
					}
					else
					{
						reverseTable.add(neighbor, src);
						remoteEdgeCount++;
					}
				}
				vertexCount++;
				if (verbose && (vertexCount % progressStep == 0))
				{
					System.out.println("Source vertices processed: " + vertexCount
							+ ", local edges: " + localEdgeCount + ", remote edges: "
							+ remoteEdgeCount);
				}
			}
			if (verbose)
			{
				System.out.println("Waiting for completion of remote add operations.");
			}
			graph.ensureRemoteAddsHaveCompleted();
			if (verbose)
			{
				System.out.println("Reverse adjacency table completed.");
				System.out.println("Source vertices processed: " + vertexCount
						+ ", local edges: " + localEdgeCount + ", remote edges: "
						+ remoteEdgeCount);
			}

			return null;
		}
	}
	
	static class ReverseSizeComputation extends LiveDistributedObject<Serializable, Serializable>
	{
		private BigAdjacencyTable adjTable;
		DHTAllocation allocation;
		private IntDHT sizes;
		private boolean verbose = false;
		
		public ReverseSizeComputation(BigAdjacencyTable adjTable, IntDHT sizes, boolean verbose)
		{
			this(adjTable.getID() + "/reverseSizeComputation", adjTable.getAllocation(), adjTable.getID(), sizes.getID(), verbose);
		}
		
		protected ReverseSizeComputation(String id, Allocation allocation, String adjId, String sizesId, boolean verbose)
		{
			super(id, allocation, adjId, sizesId, verbose);
			this.adjTable = (BigAdjacencyTable) BigObjectRegistry.defaultRegistry.get(adjId);
			this.allocation = (DHTAllocation) allocation;
			this.sizes = (IntDHT) BigObjectRegistry.defaultRegistry.get(sizesId);
			this.verbose = verbose;
		}
		
		void execute()
		{
			new OneNodeOneRequest<NoReturn>()
			{
				@Override
				protected ComputationRequest<NoReturn> createComputationRequestForNode(
						OctojusNode n)
				{
					return new ExecutionRequest(getID());
				}
			}.execute(allocation.getNodes());
		}
		
		@SuppressWarnings("serial")
		static private class ExecutionRequest extends BigObjectComputingRequest<ReverseSizeComputation, NoReturn>
		{
			public ExecutionRequest(String bigObjectID)
			{
				super(bigObjectID);
			}

			@Override
			protected NoReturn localComputation(ReverseSizeComputation object)
					throws Throwable
			{
				object.localExecute();
				return null;
			}
		}
		
		void localExecute()
		{
			if (verbose)
			{
				System.out.println("Counting IN edges.");
			}
			int nVertices = adjTable.getLocalMap().size();
			int progressStep = (nVertices / (10 * 10000)) * 10000;
			progressStep = Math.min( Math.max(progressStep, 100 * 1000), 100 * 1000 * 1000);
			int vertexCount = 0;
			int localEdgeCount = 0;
			int remoteEdgeCount = 0;
			// Iterate over the local part of the adjacency table and count the
			// neighboring size while considering the reverse edges.
			for (LongObjectCursor<LongSet> srcCursor : adjTable.getLocalMap())
			{
				@SuppressWarnings("unused")
				long src = srcCursor.key;
				LongSet neighbors = srcCursor.value;
				for (LongCursor neighborCursor : neighbors)
				{
					long neighbor = neighborCursor.value;
					if (allocation.isLocalElement(neighbor))
					{
						synchronized (sizes.getLocalData())
						{
							sizes.getLocalData().put(neighbor, sizes.getLocalData().get(neighbor) + 1);
						}
						localEdgeCount++;
					}
					else
					{
						OctojusNode clusterNode = allocation.getOwnerNode(neighbor);
						TCPConnection connection = connectTo(incrementNeighboringSize, clusterNode);
						try
						{
							connection.out.writeBoolean(false);
							connection.out.writeLong(neighbor);
						}
						catch (IOException e)
						{
							e.printStackTrace();
							break;
						}
						remoteEdgeCount++;
					}
				}
				vertexCount++;
				if (verbose && (vertexCount % progressStep == 0))
				{
					System.out.println("Source vertices processed: " + vertexCount
							+ ", local edges: " + localEdgeCount + ", remote edges: "
							+ remoteEdgeCount);
				}
			}
			if (verbose)
			{
				System.out.println("Waiting for completion of remote counting operations.");
			}
			// Send true to all nodes to indicate the end of the work
			// and wait for their response.
			for (OctojusNode node : allocation.getNodes())
			{
				if ( ! node.isLocalNode())
				{
					TCPConnection connection = connectTo(incrementNeighboringSize, node);
					try
					{
						connection.out.writeBoolean(true);
						connection.out.flush();
					}
					catch (IOException e)
					{
						e.printStackTrace();
						break;
					}
				}
			}
			// wait for response from all nodes
			for (OctojusNode node : allocation.getNodes())
			{
				if ( ! node.isLocalNode())
				{
					TCPConnection connection = connectTo(incrementNeighboringSize, node);
					try
					{
						@SuppressWarnings("unused")
						boolean finished = connection.in.readBoolean();
					}
					catch (IOException e)
					{
						e.printStackTrace();
					}
				}
			}
			if (verbose)
			{
				System.out.println("Reverse adjacency table size computation completed.");
				System.out.println("Source vertices processed: " + vertexCount
						+ ", local edges: " + localEdgeCount + ", remote edges: "
						+ remoteEdgeCount);
			}
		}
		
		private Service incrementNeighboringSize = new Service()
		{
			@Override
			public void serveThrough(FullDuplexDataConnection2 connection) throws ClassNotFoundException,
					IOException
			{
				boolean end = connection.in.readBoolean();
				if ( ! end)
				{
					long vertex = connection.in.readLong();
					synchronized (sizes.getLocalData())
					{
						sizes.getLocalData().put(vertex, sizes.getLocalData().get(vertex) + 1);
					}

				}
				else
				{
					connection.out.writeBoolean(true);
				}
			}
		};
		
		@Override
		public void clearLocalData()
		{
			setLocalData(null);
		}
		
	}
	
}
