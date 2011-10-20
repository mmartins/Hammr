package launcher;

import java.util.Iterator;

import java.util.Map;
import java.util.HashMap;

import java.rmi.RemoteException;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import java.net.InetSocketAddress;

import appspecs.Node;

import execinfo.NodeGroup;
import execinfo.ResultSummary;
import execinfo.NodeMeasurements;

import communication.ChannelHandler;

import communication.SHMChannelHandler;
import communication.TCPChannelHandler;
import communication.FileChannelHandler;

import communication.SHMRecordMultiplexer;
import communication.SHMRecordWriter;

import communication.TCPRecordMultiplexer;
import communication.TCPRecordWriter;

import communication.FileRecordReader;
import communication.FileRecordWriter;

import interfaces.Manager;

import exceptions.InexistentApplicationException;

/**
 * This class is responsible for running a specific NodeGroup previously submitted to the Launcher.
 * 
 * @author Hammurabi Mendes (hmendes)
 * @author Marcelo Martins (martins)
 */
public class ExecutionHandler extends Thread {
	private Manager manager;

	private ConcreteLauncher concreteLauncher;

	private NodeGroup nodeGroup;

	/**
	 * Constructor.
	 * 
	 * @param manager Reference to manager.
	 * @param concreteLauncher Reference to local launcher.
	 * @param nodeGroup NodeGroup that should run.
	 */
	public ExecutionHandler(Manager manager, ConcreteLauncher concreteLauncher, NodeGroup nodeGroup) {
		this.manager = manager;

		this.concreteLauncher = concreteLauncher;

		this.nodeGroup = nodeGroup;
	}

	/**
	 * Setter for NodeGroup
	 * 
	 * @param nodeGroup NodeGroup that should run.
	 */
	public void setNodeGroup(NodeGroup nodeGroup) {
		this.nodeGroup = nodeGroup;
	}

	/**
	 * Getter for the NodeGroup
	 * 
	 * @return NodeGroup that should run.
	 */
	public NodeGroup getNodeGroup() {
		return nodeGroup;
	}

	/**
	 * Runs NodeGroup members in separate threads, one for each Node.
	 */
	public void run() {
		// Store runtime information; sent back to master
		// at the end of execution.
		ResultSummary resultSummary;

		try {
			setupCommunication();
		} catch (Exception genericException) {
			System.err.println("Error setting up communication for NodeGroup");

			genericException.printStackTrace();

			resultSummary = new ResultSummary(nodeGroup.getApplicationName(), nodeGroup.getSerialNumber(), ResultSummary.Type.FAILURE);

			finishExecution(resultSummary);

			return;
		}

		resultSummary = performExecution();

		finishExecution(resultSummary);
	}

	/**
	 * Creates communication channels for NodeGroup
	 * If Node has server-side TCP channel, notify master about obtained socket
	 * address.
	 * If Node has client-side TCP channel, obtains from master the associated
	 * socket address.
	 * 
	 * @throws Exception
	 *             If one of the following situations occur
	 *             1) Error creating client-side or server-side TCP channels;
	 *             2) Address of server-side TCP channel required by NodeGroup 
	 *             is not registered in master;
	 *             3) Error creating or opening file channels.
	 */
	private void setupCommunication() throws Exception {
		/*
		 * Create all pipe handlers
		 * If two pipe edges target the same node, only one pipe handler (and
		 * corresponding physical pipe) is created
		 */

		Map<String, SHMRecordMultiplexer> mapRecordOutputStream = new HashMap<String, SHMRecordMultiplexer>();

		for (Node node: nodeGroup.getNodes()) {
			SHMRecordMultiplexer shmRecordMultiplexer = null;

			for (ChannelHandler channelHandler: node.getInputChannels()) {
				if (channelHandler.getType() == ChannelHandler.Type.SHM) {
					SHMChannelHandler shmChannelHandler = (SHMChannelHandler) channelHandler;

					if (shmRecordMultiplexer == null) {
						shmRecordMultiplexer = new SHMRecordMultiplexer(node.getInputChannelNames());

						/*
						 * For SHM, when creating input pipe, map the
						 * associated output pipe for destination nodes
						 */
						mapRecordOutputStream.put(node.getName(), shmRecordMultiplexer);
					}

					// For SHM, all inputs come from the unique input pipe
					shmChannelHandler.setRecordReader(shmRecordMultiplexer);
				}
			}
		}

		for (Node node: nodeGroup.getNodes()) {
			for (ChannelHandler channelHandler: node.getOutputChannels()) {
				if(channelHandler.getType() == ChannelHandler.Type.SHM) {
					SHMChannelHandler shmChannelHandler = (SHMChannelHandler) channelHandler;

					/*
					 * For SHM, all the outputs go to the unique output pipe 
					 * for each node
					 */

					SHMRecordWriter shmRecordWriter = new SHMRecordWriter(node.getName(), mapRecordOutputStream.get(channelHandler.getName()));

					shmChannelHandler.setRecordWriter(shmRecordWriter);
				}
			}
		}

		/* 
		 * Create all TCP handlers
		 * If two TCP edges target the same node, only one TCP handler
		 * (and corresponding server) is created
		 */

		for (Node node: nodeGroup.getNodes()) {
			TCPRecordMultiplexer tcpRecordMultiplexer = null;

			for (ChannelHandler channelHandler: node.getInputChannels()) {
				if (channelHandler.getType() == ChannelHandler.Type.TCP) {
					TCPChannelHandler tcpChannelHandler = (TCPChannelHandler) channelHandler;

					if (tcpRecordMultiplexer == null) {
						tcpRecordMultiplexer = new TCPRecordMultiplexer(node.getInputChannelNames());

						tcpChannelHandler.setSocketAddress(tcpRecordMultiplexer.getAddress());

						/* 
						 * For TCP, when creating the input server, map the
						 * associated output server addresses for other nodes
						 */
						boolean result = manager.registerSocketAddress(nodeGroup.getApplicationName(), node.getName(), tcpChannelHandler.getSocketAddress());

						if (result == false) {
							System.err.println("Unable to insert socket address for application " + nodeGroup.getApplicationName() + " in manager!");

							throw new InexistentApplicationException(nodeGroup.getApplicationName());
						}
					}

					// For TCP, all inputs come from the unique input server
					tcpChannelHandler.setRecordReader(tcpRecordMultiplexer);
				}
			}
		}

		for (Node node: nodeGroup.getNodes()) {
			for (ChannelHandler channelHandler: node.getOutputChannels()) {
				if (channelHandler.getType() == ChannelHandler.Type.TCP) {
					TCPChannelHandler tcpChannelHandler = (TCPChannelHandler) channelHandler;

					/*
					 *  For TCP, (1) obtain the address of the output server
					 *  from manager
					 */
					InetSocketAddress socketAddress = manager.obtainSocketAddress(nodeGroup.getApplicationName(), tcpChannelHandler.getName());

					if(socketAddress == null) {
						throw new InexistentApplicationException(nodeGroup.getApplicationName());
					}

					tcpChannelHandler.setSocketAddress(socketAddress);

					/*
					 *  For TCP, (2) all the outputs go to the unique server 
					 *  for each node
					 */

					TCPRecordWriter tcpRecordWriter = new TCPRecordWriter(node.getName(), socketAddress);

					tcpChannelHandler.setRecordWriter(tcpRecordWriter);
				}
			}
		}

		/*
		 * Create all the file handlers
		 * If more than one file edge target the same node, more than one file
		 * handler (and corresponding file descriptor) is created
		 */

		for (Node node: nodeGroup.getNodes()) {
			for (ChannelHandler channelHandler: node.getInputChannels()) {
				if (channelHandler.getType() == ChannelHandler.Type.FILE) {
					FileChannelHandler fileChannelHandler = (FileChannelHandler) channelHandler;

					FileRecordReader fileRecordReader = new FileRecordReader(fileChannelHandler.getLocation());

					fileChannelHandler.setRecordReader(fileRecordReader);
				}
			}

			for (ChannelHandler channelHandler: node.getOutputChannels()) {
				if (channelHandler.getType() == ChannelHandler.Type.FILE) {
					FileChannelHandler fileChannelHandler = (FileChannelHandler) channelHandler;

					FileRecordWriter fileRecordWriter = new FileRecordWriter(fileChannelHandler.getLocation());

					fileChannelHandler.setRecordWriter(fileRecordWriter);
				}
			}
		}
	}

	/**
	 * Performs task execution inside Nodes, one per thread, and sends result 
	 * summary to master.
	 * 
	 * @return Result summary to be sent back to master.
	 */
	private ResultSummary performExecution() {
		NodeProfileHandler[] nodeHandlers = new NodeProfileHandler[nodeGroup.size()];

		Iterator<Node> iterator = nodeGroup.iterator();

		long globalTimerStart = System.currentTimeMillis();

		for (int i = 0; i < nodeGroup.size(); i++) {
			nodeHandlers[i] = new NodeProfileHandler(iterator.next());
			nodeHandlers[i].start();
		}

		for (int i = 0; i < nodeGroup.size(); i++) {
			try {
				nodeHandlers[i].join();
			} catch (InterruptedException exception) {
				System.err.println("Unexpected thread interruption while waiting for node execution termination");

				i--;
				continue;
			}
		}

		long globalTimerFinish = System.currentTimeMillis();

		ResultSummary resultSummary = new ResultSummary(nodeGroup.getApplicationName(), nodeGroup.getSerialNumber(), ResultSummary.Type.SUCCESS);

		resultSummary.setNodeGroupTiming(globalTimerFinish - globalTimerStart);

		for (int i = 0; i < nodeGroup.size(); i++) {
			resultSummary.addNodeMeasurements(nodeHandlers[i].getNode().getName(), nodeHandlers[i].getNodeMeasurements());
		}

		return resultSummary;
	}

	/**
	 * Sends the result summary of NodeGroup to master.
	 * Clears the NodeGroup data from launcher.
	 * 
	 * @param resultSummary Result summary obtained after NodeGroup is executed.
	 * 
	 * @return True if master is properly notified; false otherwise.
	 */
	private boolean finishExecution(ResultSummary resultSummary) {
		concreteLauncher.delNodeGroup(nodeGroup);

		try {
			manager.handleTermination(resultSummary);
		} catch (RemoteException exception) {
			System.err.println("Unable to communicate termination to manager");

			exception.printStackTrace();
			return false;
		}	

		return true;
	}

	/**
	 * Class that executes a single Node in a separate thread,
	 *  performing the appropriate measurements.
	 * 
	 * @author Hammurabi Mendes (hmendes)
	 * @author Marcelo Martins (martins)
	 */
	class NodeProfileHandler extends Thread {
		private Node node;

		private long realLocalTimerStart;
		private long realLocalTimerFinish;

		private long cpuLocalTimerStart;
		private long cpuLocalTimerFinish;

		private long userLocalTimerStart;
		private long userLocalTimerFinish;

		/**
		 * Class constructor.
		 * 
		 * @param node Node to be run in a separate thread.
		 */
		public NodeProfileHandler(Node node) {
			this.node = node;
		}

		/**
		 * Runs Node in a separate thread and obtain runtime measurements.
		 */
		public void run() {
			System.out.println("Executing " + node);

			ThreadMXBean profiler = ManagementFactory.getThreadMXBean();

			if (profiler.isThreadCpuTimeSupported()) {
				if (!profiler.isThreadCpuTimeEnabled()) {
					profiler.setThreadCpuTimeEnabled(true);
				}
			}

			realLocalTimerStart = System.currentTimeMillis();

			cpuLocalTimerStart = profiler.getCurrentThreadCpuTime();
			userLocalTimerStart = profiler.getCurrentThreadUserTime();

			node.run();

			realLocalTimerFinish = System.currentTimeMillis();

			cpuLocalTimerFinish = profiler.getCurrentThreadCpuTime();
			userLocalTimerFinish = profiler.getCurrentThreadUserTime();
		}

		/**
		 * Getter for running Node
		 * 
		 * @return running Node.
		 */
		public Node getNode() {
			return node;
		}

		/**
		 * Getter for Node's real-time execution.
		 * 
		 * @return The real time to execute Node.
		 */
		public long getRealTime() {
			return realLocalTimerFinish - realLocalTimerStart;
		}

		/**
		 * Getter for Node's CPU time.
		 * 
		 * @return The CPU time to execute Node.
		 */
		public long getCpuTime() {
			// Return time in milliseconds, not in nanoseconds
			
			return (cpuLocalTimerFinish - cpuLocalTimerStart) / 1000000;
		}

		/**
		 * Getter for Node's user time
		 * 
		 * @return The user time to execute Node.
		 */
		public long getUserTime() {
			// Return time in milliseconds, not nanoseconds
			
			return (userLocalTimerFinish - userLocalTimerStart) / 1000000;
		}

		/**
		 * Getter for the whole set of node measurements.
		 * 
		 * @return The whole set of node measurements.
		 */
		public NodeMeasurements getNodeMeasurements() {
			return new NodeMeasurements(getRealTime(), getCpuTime(), getUserTime());
		}
	}
}
