package biggrph;

import java.util.ArrayList;
import java.util.List;

import bigobject.LDJOClusterMonitor;
import octojus.OctojusCluster;
import oscilloscup.multiscup.Multiscope;
import oscilloscup.multiscup.Property;
import oscilloscup.render.FigureRenderer;

public class BigGrphClusterMonitor extends LDJOClusterMonitor
{
	private final Multiscope<BigAdjacencyTable> view = new Multiscope<BigAdjacencyTable>(
			getBigGrphProps())
	{

		@Override
		protected String getRowNameFor(BigAdjacencyTable bo)
		{
			return bo.getID();
		}

		@Override
		protected FigureRenderer getSpecificRenderer(BigAdjacencyTable row,
				Property<BigAdjacencyTable> property)
		{
			return null;
		}
	};

	public BigGrphClusterMonitor(OctojusCluster cluster, int refreshPeriodMs)
	{
		super(cluster, refreshPeriodMs);

		addMonitoringPane(view, "adj tables");
		view.setRefreshPeriodMs(refreshPeriodMs);
	}

	private List<Property<BigAdjacencyTable>> getBigGrphProps()
	{
		List<Property<BigAdjacencyTable>> props = new ArrayList<>();

		Property.addProperty(props, new Property<BigAdjacencyTable>("edge locality", null)
		{

			@Override
			public Object getRawValue(BigAdjacencyTable n)
			{
				return n.__getEdgeLocality();
			}
		});

		return props;
	}
}
