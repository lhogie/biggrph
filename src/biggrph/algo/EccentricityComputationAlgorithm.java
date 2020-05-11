package biggrph.algo;

import java.io.Serializable;
import java.util.Map;
import java.util.Random;

import octojus.ComputationRequest;
import octojus.NoReturn;
import octojus.OctojusNode;
import octojus.OneNodeOneRequest;
import toools.collection.bigstuff.longset.LongCursor;
import toools.math.Distribution;
import toools.util.LongPredicate;
import biggrph.GraphTopology;
import biggrph.algo.degree.MaxDegreeAlgorithmResult;
import bigobject.BigObjectReference;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.LongIntCursor;
import com.carrotsearch.hppc.cursors.LongObjectCursor;

import dht.ByteDHT;
import dht.IntDHT;
import dht.KeyValuePair;
import dht.LongDHT;
import dht.set.LongSet;

/**
 * Implementation of the algorithm described in Computing the Eccentricity
 * Distribution of Large Graphs, Frank W. Takes and WAlter Kosters, Algorithms
 * 2013, 6. http://liacs.leidenuniv.nl/~kosterswa/algorithms-06-00100.pdf
 * 
 * @author nchleq
 *
 */
@SuppressWarnings("serial")
public class EccentricityComputationAlgorithm implements Serializable
{

	public EccentricityComputationAlgorithm(GraphTopology graph)
	{
		this(graph, LongPredicate.ACCEPT_ALL);
	}

	public EccentricityComputationAlgorithm(GraphTopology graph, LongPredicate filter)
	{
		this.graph = new BigObjectReference<GraphTopology>(graph.getID());
		this.globalFilter = (filter != null ? filter : LongPredicate.ACCEPT_ALL);
		long nVertices = graph.getNumberOfVertices();
		IntDHT low = new IntDHT(graph.getID() + "/eccentricity/lowerBounds",
				graph.getAllocation(), nVertices);
		IntDHT upp = new IntDHT(graph.getID() + "/eccentricity/upperBounds",
				graph.getAllocation(), nVertices);
		this.lowerBounds = new BigObjectReference<IntDHT>(low.getID());
		this.upperBounds = new BigObjectReference<IntDHT>(upp.getID());
	}

	private BigObjectReference<GraphTopology> graph;

	private LongPredicate globalFilter;

	private BigObjectReference<IntDHT> lowerBounds;
	private BigObjectReference<IntDHT> upperBounds;
	private BigObjectReference<IntDHT> eccentricities;

	private boolean verbose = false;

	public void setVerbose(boolean verbose)
	{
		this.verbose = verbose;
	}

	public IntDHT execute()
	{
		long nVertices = graph.get().getNumberOfVertices(globalFilter);
		IntDHT eccentricityValues = new IntDHT(graph.get().getID()
				+ "/eccentricity/values", graph.get().getAllocation(), nVertices);
		execute(eccentricityValues);
		return eccentricityValues;
	}

	public void execute(IntDHT eccentricityValues)
	{
		EccUpdatePhaseCounters globalCounters = new EccUpdatePhaseCounters();
		this.eccentricities = new BigObjectReference<IntDHT>(eccentricityValues.getID());
		initializeBounds();
		long nVertices = graph.get().getNumberOfVertices(globalFilter);
		int nIterations = 0;
		while (eccentricities.get().size() < nVertices)
		{
			long source = selectVertex(eccentricities.get());
			LongDHT distances = graph.get().getOutAdjacencyTable()
					.bfs(source, false, globalFilter).distanceDHT;
			int sourceEccentricity = distances.maxValue(globalFilter).getValue()
					.intValue();
			eccentricities.get().set(source, sourceEccentricity);
			EccUpdatePhaseCounters counters = updateEccentricities(distances,
					sourceEccentricity);
			globalCounters.add(counters);
			distances.delete();
			nIterations += 1;
			if (verbose)
			{
				long nAssigned = eccentricities.get().size();
				float ratio = (float) nAssigned / (float) nVertices;
				System.out.println("Eccentricity Computation: iteration " + nIterations
						+ ", " + eccentricities.get().size() + " vertices assigned ("
						+ ratio * 100 + "%).");
			}
		}
		if (verbose)
		{
			System.out.println("Iterations: " + nIterations);
			System.out.println("Vertices assigned by bound refinement: "
					+ globalCounters.nVerticesAssigned);
			System.out.println("Vertices assigned by pruning: "
					+ globalCounters.nVerticesPruned);
		}
	}

	public IntDHT executeExhaustiveAlgorithm()
	{
		long nVertices = graph.get().getNumberOfVertices();
		IntDHT eccentricityValues = new IntDHT(graph.get().getID()
				+ "/eccentricity/values", graph.get().getAllocation(), nVertices);
		executeExhaustiveAlgorithm(eccentricityValues);
		return eccentricityValues;
	}

	public void executeExhaustiveAlgorithm(IntDHT eccentricityValues)
	{
		for (LongObjectCursor<LongSet> cursor : graph.get().getOutAdjacencyTable()
				.getLocalData())
		{
			long vertex = cursor.key;
			int eccentricity = computeExactEccentricity(vertex);
			eccentricityValues.set(vertex, eccentricity);
		}
	}

	public Distribution<Integer> executeHybridAlgorithm(float samplingFactor)
	{
		long nVertices = graph.get().getNumberOfVertices(globalFilter);
		IntDHT eccentricityValues = new IntDHT(graph.get().getID()
				+ "/hybridEccentricity/values", graph.get().getAllocation(), nVertices);
		int[] globalBounds = executeLRBoundingAlgorithm(eccentricityValues);
		final int globalLowerBound = globalBounds[0];
		final int globalUpperBound = globalBounds[1];
		final ByteDHT samplingCandidates = new ByteDHT(graph.get().getID()
				+ "/hybridEccentricity/sampling", graph.get().getAllocation(), nVertices);
		markSamplingCandidates(globalLowerBound, globalUpperBound, eccentricityValues,
				samplingCandidates);
		Distribution<Integer> eccDistribution = eccentricityValues
				.computeDistribution(globalFilter.and(new InCenterOrPeripheryPredicate(
						globalLowerBound, globalUpperBound, eccentricityValues)));
		long maxVertexSampled = (long) ((double) samplingCandidates.size() * samplingFactor);
		int delta = (int) Math.round(1.0f / samplingFactor);
		long vertexSampled = 0;
		Random random = new Random();
		if (verbose)
		{
			System.out.println("Sampling " + maxVertexSampled + " vertices in "
					+ samplingCandidates.size() + " candidates.");
		}
		long effectiveComputations = 0;
		while (vertexSampled < maxVertexSampled)
		{
			long vertex;
			do
			{
				vertex = graph.get().pickRandomVertex(random);
			}
			while (samplingCandidates.get(vertex) == 0);

			int vertexEccentricity = eccentricityValues.get(vertex);
			if (vertexEccentricity == 0)
			{
				vertexEccentricity = computeExactEccentricity(vertex);
				eccentricityValues.set(vertex, vertexEccentricity);
				effectiveComputations += 1;
			}
			eccDistribution.addNOccurences(vertexEccentricity, delta);
			samplingCandidates.set(vertex, (byte) 0);
			vertexSampled += 1;
		}
		if (verbose)
		{
			System.out.println("Sampling: " + vertexSampled + " vertices sampled, "
					+ effectiveComputations + " eccentricity computations done.");
		}
		samplingCandidates.delete();
		eccentricityValues.delete();
		return eccDistribution;
	}

	private void markSamplingCandidates(int globalLowerBound, int globalUpperBound,
			IntDHT eccentricityValues, ByteDHT samplingCandidates)
	{
		new OneNodeOneRequest<NoReturn>()
		{
			@Override
			protected ComputationRequest<NoReturn> createComputationRequestForNode(
					OctojusNode n)
			{
				return new MarkSamplingCandidatesRequest(
						EccentricityComputationAlgorithm.this, globalLowerBound,
						globalUpperBound, eccentricityValues, samplingCandidates);
			}
		}.execute(graph.get().getAllocation().getNodes());
	}

	static private class MarkSamplingCandidatesRequest extends
			ComputationRequest<NoReturn>
	{
		private EccentricityComputationAlgorithm ecc;
		int globalLowerBound;
		int globalUpperBound;
		BigObjectReference<IntDHT> eccentricityValues;
		BigObjectReference<ByteDHT> samplingCandidates;

		public MarkSamplingCandidatesRequest(EccentricityComputationAlgorithm ecc,
				int globalLowerBound, int globalUpperBound, IntDHT eccentricityValues,
				ByteDHT samplingCandidates)
		{
			this.ecc = ecc;
			this.globalLowerBound = globalLowerBound;
			this.globalUpperBound = globalUpperBound;
			this.eccentricityValues = new BigObjectReference<IntDHT>(
					eccentricityValues.getID());
			this.samplingCandidates = new BigObjectReference<ByteDHT>(
					samplingCandidates.getID());
		}

		@Override
		protected NoReturn compute() throws Throwable
		{
			for (LongObjectCursor<LongSet> cursor : ecc.graph.get().getLocalMap())
			{
				long vertex = cursor.key;
				if (ecc.globalFilter.accept(vertex))
				{
					if (eccentricityValues.get().containsKey(vertex))
					{
						int currentEccentricity = eccentricityValues.get().get(vertex);
						if (globalLowerBound < currentEccentricity
								&& currentEccentricity < globalUpperBound)
						{
							samplingCandidates.get().set(vertex, (byte) 1);
						}
					}
					else
					{
						samplingCandidates.get().set(vertex, (byte) 1);
					}
				}
			}
			return null;
		}
	}

	private int computeExactEccentricity(long vertex)
	{
		LongDHT distances = graph.get().getOutAdjacencyTable()
				.bfs(vertex, false, globalFilter).distanceDHT;
		int vertexEccentricity = distances.maxValue(globalFilter).getValue().intValue();
		distances.delete();
		return vertexEccentricity;
	}

	private int[] executeLRBoundingAlgorithm(IntDHT eccentricityValues)
	{
		EccUpdatePhaseCounters globalCounters = new EccUpdatePhaseCounters();
		this.eccentricities = new BigObjectReference<IntDHT>(eccentricityValues.getID());
		initializeBounds();
		long nVertices = graph.get().getNumberOfVertices();
		int nIterations = 0;
		int globalLowerBound = Integer.MIN_VALUE;
		int globalUpperBound = Integer.MAX_VALUE;

		while (countVerticesInCenterOrPeriphery(globalLowerBound, globalUpperBound) > 0)
		{
			long source = selectVertex(eccentricities.get());
			LongDHT distances = graph.get().getOutAdjacencyTable()
					.bfs(source, false, globalFilter).distanceDHT;
			int sourceEccentricity = (int) distances.maxValue(globalFilter).getValue()
					.longValue();
			eccentricities.get().set(source, sourceEccentricity);
			lowerBounds.get().set(source, sourceEccentricity);
			upperBounds.get().set(source, sourceEccentricity);
			EccUpdatePhaseCounters counters = updateEccentricities(distances,
					sourceEccentricity);
			globalCounters.add(counters);
			distances.delete();
			globalLowerBound = lowerBounds.get().minValue(globalFilter).getValue();
			globalUpperBound = upperBounds.get().maxValue(globalFilter).getValue();
			nIterations += 1;
			if (verbose)
			{
				long nAssigned = eccentricities.get().size();
				float ratio = (float) nAssigned / (float) nVertices;
				System.out.println("Eccentricity LRBounding Computation: iteration "
						+ nIterations + ", " + eccentricities.get().size()
						+ " vertices assigned (" + ratio * 100 + "%)" + ", LBR = "
						+ counters.nLowerBoundsRefined + ", UBR = "
						+ counters.nUpperBoundsRefined + ", Pruned = "
						+ counters.nVerticesPruned + ", l = " + globalLowerBound
						+ ", r = " + globalUpperBound);
			}
		}
		globalLowerBound = (int) lowerBounds
				.get()
				.minValue(
						new NotAssignedVertexPredicate(eccentricityValues)
								.and(globalFilter)).getValue() - 1;
		globalUpperBound = (int) upperBounds
				.get()
				.maxValue(
						new NotAssignedVertexPredicate(eccentricityValues)
								.and(globalFilter)).getValue() + 1;
		if (verbose)
		{
			System.out.println("L = " + globalLowerBound + ", R = " + globalUpperBound);
		}
		int[] res = { globalLowerBound, globalUpperBound };
		return res;
	}

	private long countVerticesInCenterOrPeriphery(int globalLowerBound,
			int globalUpperBound)
	{
		// Implements the counting described in the paper : how many vertices in
		// the W set have lowerBound[v] == globalLowerBound or upperBound[v] ==
		// globalUpperBound
		Map<OctojusNode, Long> localCounts = new OneNodeOneRequest<Long>()
		{
			@Override
			protected ComputationRequest<Long> createComputationRequestForNode(
					OctojusNode n)
			{
				return new CountVerticesInCenterOrPeripheryRequest(lowerBounds.get(),
						upperBounds.get(), eccentricities.get(), globalLowerBound,
						globalUpperBound);
			}
		}.execute(graph.get().getAllocation().getNodes());
		long globalCount = 0;
		for (long lc : localCounts.values())
		{
			globalCount += lc;
		}
		return globalCount;
	}

	private static class CountVerticesInCenterOrPeripheryRequest extends
			ComputationRequest<Long>
	{
		private BigObjectReference<IntDHT> lowerBounds;
		private BigObjectReference<IntDHT> upperBounds;
		private BigObjectReference<IntDHT> eccentricities;

		private int globalLowerBound;
		private int globalUpperBound;

		public CountVerticesInCenterOrPeripheryRequest(IntDHT lowerBounds,
				IntDHT upperBounds, IntDHT eccentricities, int globalLowerBound,
				int globalUpperBound)
		{
			this.lowerBounds = new BigObjectReference<IntDHT>(lowerBounds.getID());
			this.upperBounds = new BigObjectReference<IntDHT>(upperBounds.getID());
			this.eccentricities = new BigObjectReference<IntDHT>(eccentricities.getID());
			this.globalLowerBound = globalLowerBound;
			this.globalUpperBound = globalUpperBound;
		}

		@Override
		protected Long compute() throws Throwable
		{
			long count = 0;
			for (LongIntCursor cursor : lowerBounds.get().getLocalMap())
			{
				// vertex w is in W, and its bounds are outside the global
				// window
				if ( ! eccentricities.get().containsKey(cursor.key))
				{
					int lowerBound = cursor.value;
					int upperBound = upperBounds.get().get(cursor.key);
					if (lowerBound == globalLowerBound || upperBound == globalUpperBound)
					{
						count++;
					}
				}
			}
			return count;
		}
	}

	void initializeBounds()
	{
		new OneNodeOneRequest<NoReturn>()
		{
			@Override
			protected ComputationRequest<NoReturn> createComputationRequestForNode(
					OctojusNode n)
			{
				return new InitializeBoundsRequest(EccentricityComputationAlgorithm.this);
			}
		}.execute(graph.get().getAllocation().getNodes());
	}

	static class InitializeBoundsRequest extends ComputationRequest<NoReturn>
	{
		EccentricityComputationAlgorithm ecc;

		public InitializeBoundsRequest(
				EccentricityComputationAlgorithm eccentricityComputationAlgorithm)
		{
			this.ecc = eccentricityComputationAlgorithm;
		}

		@Override
		protected NoReturn compute() throws Throwable
		{
			ecc.localInitializeBounds();
			return null;
		}
	}

	void localInitializeBounds()
	{
		LongObjectOpenHashMap<LongSet> localVertices = graph.get().getOutAdjacencyTable()
				.getLocalMap();
		for (LongObjectCursor<LongSet> cr : localVertices)
		{
			lowerBounds.get().set(cr.key, Integer.MIN_VALUE);
			upperBounds.get().set(cr.key, Integer.MAX_VALUE);
		}
	}

	EccUpdatePhaseCounters updateEccentricities(LongDHT distances, int sourceEccentricity)
	{
		Map<OctojusNode, EccUpdatePhaseCounters> localCounters = new OneNodeOneRequest<EccUpdatePhaseCounters>()
		{
			@Override
			protected ComputationRequest<EccUpdatePhaseCounters> createComputationRequestForNode(
					OctojusNode n)
			{
				return new UpdateEccentricityRequest(distances, sourceEccentricity,
						EccentricityComputationAlgorithm.this);
			}
		}.execute(graph.get().getAllocation().getNodes());
		EccUpdatePhaseCounters result = null;
		for (EccUpdatePhaseCounters localRes : localCounters.values())
		{
			if (result == null)
				result = localRes;
			else
				result = result.add(localRes);
		}
		return result;
	}

	static class UpdateEccentricityRequest extends
			ComputationRequest<EccUpdatePhaseCounters>
	{
		private BigObjectReference<LongDHT> distancesReference;
		private int sourceEccentricity;
		private EccentricityComputationAlgorithm ecc;

		public UpdateEccentricityRequest(LongDHT distances, int sourceEccentricity,
				EccentricityComputationAlgorithm ecc)
		{
			this.distancesReference = new BigObjectReference<LongDHT>(distances.getID());
			this.sourceEccentricity = sourceEccentricity;
			this.ecc = ecc;
		}

		@Override
		protected EccUpdatePhaseCounters compute() throws Throwable
		{
			return ecc.localUpdateEccentricities(distancesReference.get(),
					sourceEccentricity, ecc.eccentricities.get());
		}

	}

	static class EccUpdatePhaseCounters implements Serializable
	{
		long nLowerBoundsRefined = 0;
		long nUpperBoundsRefined = 0;
		long nVerticesAssigned = 0;
		long nVerticesPruned = 0;

		EccUpdatePhaseCounters add(EccUpdatePhaseCounters other)
		{
			this.nLowerBoundsRefined += other.nLowerBoundsRefined;
			this.nUpperBoundsRefined += other.nUpperBoundsRefined;
			this.nVerticesAssigned += other.nVerticesAssigned;
			this.nVerticesPruned += other.nVerticesPruned;
			return this;
		}
	}

	EccUpdatePhaseCounters localUpdateEccentricities(LongDHT distances,
			int sourceEccentricity, IntDHT eccentricityValues)
	{
		EccUpdatePhaseCounters counters = new EccUpdatePhaseCounters();
		GraphTopology topology = graph.get();
		LongObjectOpenHashMap<LongSet> localVertices = topology.getOutAdjacencyTable()
				.getLocalMap();
		for (LongObjectCursor<LongSet> cursor : localVertices)
		{
			long vertex = cursor.key;
			if ( ! globalFilter.accept(vertex))
			{
				continue;
			}
			// Take care of not processing vertices that were not reached during
			// the
			// BFS : they have no related entry in the distances map.
			if ( ! eccentricityValues.containsKey(vertex)
					&& distances.containsKey(vertex))
			{
				int distance = (int) distances.get(vertex);
				int lowerBefore = lowerBounds.get().get(vertex);
				int lowerAfter = Math.max(lowerBefore,
						Math.max(sourceEccentricity - distance, distance));
				if (lowerBefore != lowerAfter)
				{
					lowerBounds.get().set(vertex, lowerAfter);
					counters.nLowerBoundsRefined += 1;
				}
				int upperBefore = upperBounds.get().get(vertex);
				int upperAfter = Math.min(upperBefore, sourceEccentricity + distance);
				if (upperBefore != upperAfter)
				{
					upperBounds.get().set(vertex, upperAfter);
					counters.nUpperBoundsRefined += 1;
				}
				if (lowerAfter == upperAfter)
				{
					eccentricityValues.set(vertex, lowerAfter);
					counters.nVerticesAssigned += 1;
					// Now prune vertices with obvious eccentricities
					for (LongCursor ncr : topology.getOutNeighbors(vertex))
					{
						long neighbor = ncr.value;
						if ( ! eccentricityValues.containsKey(neighbor)
								&& globalFilter.accept(neighbor)
								&& topology.getAllocation().isLocalElement(neighbor))
						{
							int neighborDegree = topology.getOutNeighbors(neighbor)
									.size();
							if (neighborDegree == 1)
							{
								eccentricityValues.set(neighbor, lowerAfter + 1);
								lowerBounds.get().set(neighbor, lowerAfter + 1);
								upperBounds.get().set(neighbor, lowerAfter + 1);
								counters.nVerticesPruned += 1;
							}
						}
						else
						{

						}
					}
				}
			}
		}
		return counters;
	}

	static private enum selectionStrategyType
	{
		SELECT_LARGEST_DEGREE, SELECT_SMALL_LOWER, SELECT_LARGE_UPPER
	};

	private transient selectionStrategyType selectionStrategy = selectionStrategyType.SELECT_LARGEST_DEGREE;

	private long selectVertex(IntDHT eccentricityValues)
	{
		LongPredicate vertexPredicate = new NotAssignedVertexPredicate(eccentricityValues);

		if (selectionStrategy == selectionStrategyType.SELECT_LARGE_UPPER)
		{
			KeyValuePair<Integer> kv = upperBounds.get().maxValue(
					vertexPredicate.and(globalFilter));
			selectionStrategy = selectionStrategyType.SELECT_SMALL_LOWER;
			return kv.key;
		}
		else if (selectionStrategy == selectionStrategyType.SELECT_SMALL_LOWER)
		{
			KeyValuePair<Integer> kv = lowerBounds.get().minValue(
					vertexPredicate.and(globalFilter));
			selectionStrategy = selectionStrategyType.SELECT_LARGE_UPPER;
			return kv.key;
		}
		else if (selectionStrategy == selectionStrategyType.SELECT_LARGEST_DEGREE)
		{
			MaxDegreeAlgorithmResult res = graph.get().getOutMaxDegree(
					vertexPredicate.and(globalFilter));
			selectionStrategy = selectionStrategyType.SELECT_LARGE_UPPER;
			return res.vertex;
		}
		return 0;
	}

	static private class NotAssignedVertexPredicate implements LongPredicate
	{
		private BigObjectReference<IntDHT> eccReference;

		@SuppressWarnings("unused")
		NotAssignedVertexPredicate()
		{
		}

		NotAssignedVertexPredicate(IntDHT eccentricities)
		{
			eccReference = new BigObjectReference<IntDHT>(eccentricities.getID());
		}

		@Override
		public boolean accept(long v)
		{
			return ! (eccReference.get().containsKey(v));
		}
	}

	static private class InCenterOrPeripheryPredicate implements LongPredicate
	{
		BigObjectReference<IntDHT> eccValues;

		int globalLowerBound;
		int globalUpperBound;

		public InCenterOrPeripheryPredicate(int globalLowerBound, int globalUpperBound,
				IntDHT eccentricityValues)
		{
			this.eccValues = new BigObjectReference<IntDHT>(eccentricityValues.getID());
			this.globalLowerBound = globalLowerBound;
			this.globalUpperBound = globalUpperBound;
		}

		@Override
		public boolean accept(long v)
		{
			int currentEccentricity = eccValues.get().get(v);
			if (eccValues.get().containsKey(v)
					&& (currentEccentricity <= globalLowerBound || currentEccentricity >= globalUpperBound))
			{
				return true;
			}
			else
			{
				return false;
			}
		}
	}

}
