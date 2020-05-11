package biggrph;

import java.io.Serializable;

import toools.io.FullDuplexDataConnection2;
import toools.net.TCPConnection;
import bigobject.Service;
import dht.DHTAllocation;
import dht.ObjectDHT;
import dht.SerializableLongObjectMap;

public class EdgeObjectAttributeTable<E extends Serializable> extends ObjectDHT<SerializableLongObjectMap<E>>
{

	public EdgeObjectAttributeTable(String id, DHTAllocation allocation)
	{
		super(id, allocation);
	}

	@SuppressWarnings("unchecked")
	public E getEdgeAttribute(long src, long dst)
	{
		if (isLocalElement(src))
		{
			SerializableLongObjectMap<E> map = get(src);
			return (map != null ? map.get(dst) : null);
		}
		else
		{
			TCPConnection c = connectTo(getEdgeAttributeService, src);
			c.out.writeLong2(src);
			c.out.writeLong2(dst);
			// retrieves and returns the attribute value
			return (E) c.in.readObject2();
		}
	}
	
	private Service getEdgeAttributeService = new Service()
	{
		@Override
		public void serveThrough(toools.io.FullDuplexDataConnection2 c)
		{
			long src = c.in.readLong2();
			long dst = c.in.readLong2();
			SerializableLongObjectMap<E> map = getLocalData().get(src);
			E value = (map != null ? map.get(dst) : null);
			c.out.writeObject2(value);
		};
	};

	public void setEdgeAttribute(long src, long dst, E attr)
	{
		if (isLocalElement(src))
		{
			synchronized (this)
			{
				SerializableLongObjectMap<E> dstMap = get(src);
				if (dstMap == null)
				{
					dstMap = new SerializableLongObjectMap<E>();
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
			c.out.writeObject2(attr);
		}
	}

	private Service setEdgeAttributeService = new Service()
	{
		@Override
		public void serveThrough(FullDuplexDataConnection2 c)
		{
			long src = c.in.readLong2();
			long dst = c.in.readLong2();
			@SuppressWarnings("unchecked")
			E attr = (E) c.in.readObject2();
			EdgeObjectAttributeTable.this.setEdgeAttribute(src, dst, attr);
		}
	};

	public boolean containsEdge(long srcVertex, long dstVertex)
	{
		if (isLocalElement(srcVertex))
		{
			SerializableLongObjectMap<E> map = get(srcVertex);
			return (map != null ? map.containsKey(dstVertex) : false);
		}
		else
		{
			TCPConnection c = connectTo(containsEdgeService, srcVertex);
			c.out.writeLong2(srcVertex);
			c.out.writeLong2(dstVertex);
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
