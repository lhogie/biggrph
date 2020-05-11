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

package biggrph;

import jacaboo.Binaries;
import jacaboo.NodeNameSet;
import jacaboo.ResourceManager;
import jacaboo.Torque;

import java.net.UnknownHostException;
import java.util.Arrays;

import javax.swing.JComponent;

import octojus.NodeMain;
import octojus.OctojusNode;
import octojus.gui.OctojusClusterMonitorPane;
import toools.io.file.Directory;
import toools.log.TimeStampingPrintStream;
import bigobject.BigObjectCluster;
import bigobject.BigObjectNodeMain;
import bigobject.LDJOClusterMonitor;

public class BigGrphCluster extends BigObjectCluster
{

	public static final Directory bigGrphDirectory = new Directory(
			Directory.getHomeDirectory(), "biggrph");

	protected BigGrphCluster(String username, OctojusNode frontal, NodeNameSet nodenames)
			throws UnknownHostException
	{
		super(username, frontal, nodenames);
	}

	@Override
	public void start()
	{
		super.start();
		System.out.println("BigGrph started!");
	}

	@Override
	protected JComponent getMonitoringComponent()
	{
		return new BigGrphClusterMonitor(this, 1000);
	}

	public static BigGrphCluster nef_dellc6220(int nbNodes, int nbCorePerNode,
			int durationS) throws UnknownHostException
	{
		return torque(System.getProperty("user.name"), "nef-frontal", nbNodes,
				nbCorePerNode, durationS, "dellc6220");
	}

	public static BigGrphCluster torque(String frontalHostname, int nbNodes,
			int nbCorePerNode, int durationS, String... parms) throws UnknownHostException
	{
		return torque(System.getProperty("user.name"), frontalHostname, nbNodes,
				nbCorePerNode, durationS, parms);
	}

	public static BigGrphCluster torque(String loginName, String frontalHostname,
			int nbNodes, int nbCorePerNode, int durationS, String... parms)
					throws UnknownHostException
	{
		init();
		OctojusNode frontal = new OctojusNode(frontalHostname, loginName);
		ResourceManager rm = new Torque(frontal);
		System.out.println("Booking " + nbCorePerNode + " core(s) on " + nbNodes
				+ " node(s) for " + durationS + "s through " + rm + " with parameters: "
				+ Arrays.asList(parms));
		NodeNameSet nodes = rm.bookNodes(nbNodes, nbCorePerNode, durationS, parms);

		System.out.println("we got the " + nodes.size() + " following nodes: " + nodes);
		NodeNameSet fullyQualifiedNames = new NodeNameSet();

		for (String nodeName : nodes)
		{
			for (int coreIndex = 0; coreIndex < nbCorePerNode; ++coreIndex)
			{
				String fullyQualifiedNodeName = nodeName + ":"
						+ (NodeMain.getListeningPort() + coreIndex) + ":"
						+ (BigObjectNodeMain.getBigObjectPort() + coreIndex);

				fullyQualifiedNames.add(fullyQualifiedNodeName);
			}
		}

		System.out.println(fullyQualifiedNames);

		BigGrphCluster cluster = new BigGrphCluster(loginName, frontal,
				fullyQualifiedNames);
		cluster.start();
		return cluster;
	}

	public static BigGrphCluster localhost(int nNodes) throws UnknownHostException
	{
		NodeNameSet names = BigObjectCluster.localhostClusterNames(nNodes, false);
		return workstations(System.getProperty("user.name"), names);
	}

	public static BigGrphCluster localhost(int nNodes, boolean includeLocalProcess)
			throws UnknownHostException
	{
		NodeNameSet names = BigObjectCluster.localhostClusterNames(nNodes,
				includeLocalProcess);
		return workstations(System.getProperty("user.name"), names);
	}

	public static BigGrphCluster localhost(String username, int nNodes,
			boolean includeLocalProcess) throws UnknownHostException
	{
		NodeNameSet names = BigObjectCluster.localhostClusterNames(nNodes,
				includeLocalProcess);
		return workstations(username, names);
	}

	public static BigGrphCluster localhost(String username)
	{
		init();
		try
		{
			BigGrphCluster cluster = new BigGrphCluster(username, null,
					new NodeNameSet("localhost"));
			// cluster.start();
			return cluster;
		}
		catch (UnknownHostException e)
		{
			throw new IllegalStateException(e);
		}
	}

	public static BigGrphCluster localhost()
	{
		return localhost(System.getProperty("user.name"));
	}

	public static BigGrphCluster workstations(String... names) throws UnknownHostException
	{
		return workstations(System.getProperty("user.name"), new NodeNameSet(names));
	}

	public static BigGrphCluster workstations(int nbProcessPerHost, String... names)
			throws UnknownHostException
	{
		return workstations(System.getProperty("user.name"), new NodeNameSet(names),
				nbProcessPerHost);
	}

	public static BigGrphCluster workstations(NodeNameSet nodeNames)
			throws UnknownHostException
	{
		return workstations(System.getProperty("user.name"), nodeNames);
	}

	public static BigGrphCluster workstations(String username, NodeNameSet nodeNames)
			throws UnknownHostException
	{
		init();
		BigGrphCluster cluster = new BigGrphCluster(username, null, nodeNames);
		// cluster.start();
		return cluster;
	}

	public static BigGrphCluster workstations(String username, NodeNameSet nodeNames,
			int nbProcessPerHost) throws UnknownHostException
	{
		init();
		NodeNameSet newNodeNames = BigObjectCluster.clusterNamesWithProcesses(nodeNames,
				nbProcessPerHost);
		BigGrphCluster cluster = new BigGrphCluster(username, null, newNodeNames);
		// cluster.start();
		return cluster;
	}

	private static void init()
	{
		TimeStampingPrintStream.timeStampStandardIOStreams();

		Binaries.setApplicationName("biggrph");
		final Thread mainThread = Thread.currentThread();

		new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				try
				{
					mainThread.join();
				}
				catch (InterruptedException e)
				{
					throw new IllegalStateException(e);
				}
				finally
				{
					System.out.println("Stopping BigGrph");
					try
					{
						Thread.sleep(1000);
					}
					catch (InterruptedException e)
					{
						e.printStackTrace();
					}
					System.exit(0);
				}
			}
		}, "mainWaiter").start();
	}

	public void setMaxMemorySizeInGigaBytes(int nGB)
	{
		setMaxMemorySizeInMegaBytes(1024 * nGB);
	}

}
