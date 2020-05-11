package biggrph.algo.search.bfs;

public interface BFSDriver
{
	// return true if the search should continue, false otherwise
	boolean continueAfterVisiting(long v, long distance);

	boolean ignoreNeighbor(long v, long distance, long neighbor);

	public static final BFSDriver defaultDriver = new BFSDriver()
	{

		@Override
		public boolean continueAfterVisiting(long v, long distance)
		{
			return true;
		}

		@Override
		public boolean ignoreNeighbor(long v, long distance, long neighbor)
		{
			return false;
		}

	};

}
