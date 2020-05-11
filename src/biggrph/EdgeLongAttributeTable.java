package biggrph;

import toools.io.FullDuplexDataConnection2;
import toools.net.TCPConnection;
import bigobject.Service;
import dht.DHTAllocation;
import dht.ObjectDHT;
import dht.SerializableLongLongMap;

public class EdgeLongAttributeTable extends ObjectDHT<SerializableLongLongMap>
{
	private long defaultValue;
	
	@SuppressWarnings("boxing")
	public EdgeLongAttributeTable(String id, DHTAllocation allocation, long defaultValue)
	{
		super(id, allocation, defaultValue);
		this.defaultValue = defaultValue;
	}

	public long getEdgeAttribute(long src, long dst)
	{
		if (isLocalElement(src))
		{
			SerializableLongLongMap map = get(src);
			return (map != null ? map.get(dst) : defaultValue);
		}
		else
		{
			TCPConnection c = connectTo(getEdgeAttributeService, src);
			c.out.writeLong2(src);
			c.out.writeLong2(dst);
			// retrieves and returns the attribute value
			return c.in.readLong2();
		}
	}
	
	private Service getEdgeAttributeService = new Service()
	{
		@Override
		public void serveThrough(toools.io.FullDuplexDataConnection2 c)
		{
			long src = c.in.readLong2();
			long dst = c.in.readLong2();
			SerializableLongLongMap map = getLocalData().get(src);
			long value = (map != null ? map.get(dst) : defaultValue);
			c.out.writeLong2(value);
		};
	};

	public void setEdgeAttribute(long src, long dst, long attr)
	{
		if (isLocalElement(src))
		{
			synchronized (this)
			{
				SerializableLongLongMap dstMap = get(src);
				if (dstMap == null)
				{
					dstMap = new SerializableLongLongMap();
					set(src, dstMap);
				}
				dstMap.put(dst, attr);
			}			
		}
		else
		{
			TCPConnection c = connectTo(setEdgeAttributeService, src);
			c.out.writeLong2(src);
			c.out.writeLong2(dst);
			c.out.writeLong2(attr);
		}
	}

	private Service setEdgeAttributeService = new Service()
	{
		@Override
		public void serveThrough(FullDuplexDataConnection2 c)
		{
			long src = c.in.readLong2();
			long dst = c.in.readLong2();
			long attr = c.in.readLong2();
			EdgeLongAttributeTable.this.setEdgeAttribute(src, dst, attr);
		}
	};
	
	public boolean containsEdge(long src, long dst)
	{
		if (isLocalElement(src))
		{
			SerializableLongLongMap map = get(src);
			return (map != null ? map.containsKey(dst) : false);
		}
		else
		{
			TCPConnection c = connectTo(containsEdgeService, src);
			c.out.writeLong2(src);
			c.out.writeLong2(dst);
			byte res = (byte) c.in.readByte2();
			return (res == 1);
		}
	}
	
	private Service containsEdgeService = new Service()
	{
		@Override
		public void serveThrough(FullDuplexDataConnection2 c)
		{
			long src = c.in.readLong2();
			long dst = c.in.readLong2();
			boolean res = containsEdge(src, dst);
			c.out.writeByte2((byte) (res ? 1 : 0));
		};
	};
}
