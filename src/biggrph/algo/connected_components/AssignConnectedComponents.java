/////////////////////////////////////////////////////////////////////////////////////////
// 
//                 Université de Nice Sophia-Antipolis  (UNS) - 
//                 Centre National de la Recherche Scientifique (CNRS)
//                 Copyright © 2015 UNS, CNRS All Rights Reserved.
// 
//     These computer program listings and specifications, herein, are
//     the property of Université de Nice Sophia-Antipolis and CNRS
//     shall not be reproduced or copied or used in whole or in part as
//     the basis for manufacture or sale of items without written permission.
//     For a license agreement, please contact:
//     <mailto: licensing@sattse.com> 
//
//
//
//     Author: Luc Hogie – Laboratoire I3S - luc.hogie@unice.fr
//
//////////////////////////////////////////////////////////////////////////////////////////

package biggrph.algo.connected_components;

import ldjo.bsp.BSPComputation;
import ldjo.bsp.Box;
import ldjo.bsp.SingleMessageBox;
import ldjo.bsp.BSPThreadSpecifics;
import ldjo.bsp.msg.LongMessage;
import octojus.NoReturn;
import toools.thread.MultiThreadPolicy;
import toools.thread.NThreadsPolicy;
import biggrph.GraphTopology;
import bigobject.BigObjectRegistry;
import dht.DHTAllocation;
import dht.LongDHT;

public final class AssignConnectedComponents extends
		BSPComputation<GraphTopology, LongMessage, NoReturn>
{
	private LongDHT connectedComponentsAssignments;

	@SuppressWarnings("unchecked")
	public AssignConnectedComponents(GraphTopology g)
	{
		this(g.getID() + "/AssignConnectedComponents", g.getAllocation(), g.getID(),
				LongMessage.class, (Class<? extends Box<LongMessage>>) SingleMessageBox.class,
				false, new NThreadsPolicy(1));
	}

	private AssignConnectedComponents(String id, DHTAllocation segments, String graphID,
			Class<LongMessage> messageClass, Class<? extends Box<LongMessage>> boxClass,
			boolean asynchronousMessaging, MultiThreadPolicy mtp)
	{
		super(id, segments, graphID, messageClass, boxClass, asynchronousMessaging, mtp, defaultMessagePacketSize);

		if (segments.getInitiatorNode().isLocalNode())
		{
			connectedComponentsAssignments = new LongDHT(getID() + "/assignments",
					getAllocation());
		}
	}

	public LongDHT getConnectedComponentsAssignments()
	{
		if (connectedComponentsAssignments == null)
		{
			connectedComponentsAssignments = (LongDHT) BigObjectRegistry.defaultRegistry
					.get(getID() + "/assignments");
		}

		return connectedComponentsAssignments;
	}

	@Override
	protected void stepStartHook(int superStep)
	{
		if (superStep == 0)
			setScheduleAllVertices(true);
	}
	
	@Override
	protected void stepEndHook(int superStep)
	{
		if (superStep == 0)
			setScheduleAllVertices(false);
	}
	
	@Override
	protected void computeLocalElement(GraphTopology g, long step, long v,
			Box<LongMessage> inbox, BSPThreadSpecifics<GraphTopology, LongMessage> tli)
	{
		if (step == 0)
		{
			getConnectedComponentsAssignments().set(v, v);
			LongMessage msg = new LongMessage();
			msg.value = v;
			tli.post(msg, g.getOutNeighbors(v));
		}
		else
		{
			long component = getConnectedComponentsAssignments().get(v);
			LongMessage msg = null;

			for (LongMessage m : inbox)
			{
				if (m.value < component)
				{
					component = m.value;
					msg = m;
				}
			}

			if (msg != null)
			{
				getConnectedComponentsAssignments().set(v, component);
				tli.post(msg, g.getOutNeighbors(v));
			}
		}
	}
	
	@Override
	protected void combine(Box<LongMessage> box, LongMessage newMessage)
	{
		if (box.isEmpty())
		{
			box.add(newMessage);
		}
		else
		{
			boolean addMessage = false;
			for (LongMessage oldMessage : box)
			{
				if (newMessage.value < oldMessage.value)
				{
					addMessage = true;
					break;
				}
			}
			if (addMessage)
			{
				box.clear();
				box.add(newMessage);
			}
		}
	}

	@Override
	protected NoReturn getLocalResult()
	{
		return null;
	}
}