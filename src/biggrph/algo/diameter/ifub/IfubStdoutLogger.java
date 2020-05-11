package biggrph.algo.diameter.ifub;

import toools.log.LogDelegator;

public class IfubStdoutLogger extends LogDelegator implements IfubListener
{
	public static IfubStdoutLogger INSTANCE = new IfubStdoutLogger();

	@Override
	public void layer(long layer)
	{
		System.out.println("IFUB: doing layer: " + layer);
	}

	@Override
	public void newLowerBound(long lowerb)
	{
		System.out.println("new lower bound: " + lowerb);
	}

}
