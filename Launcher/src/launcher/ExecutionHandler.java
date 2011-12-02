/*
Copyright (c) 2011, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package launcher;

import interfaces.Manager;
import interfaces.StateManager;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import utilities.RMIHelper;

import appspecs.Node;

import communication.channel.FileInputChannel;
import communication.channel.FileOutputChannel;
import communication.channel.InputChannel;
import communication.channel.OutputChannel;
import communication.channel.SHMInputChannel;
import communication.channel.SHMOutputChannel;
import communication.channel.TCPInputChannel;
import communication.channel.TCPOutputChannel;
import communication.readers.FileRecordReader;
import communication.readers.SHMRecordMultiplexer;
import communication.readers.TCPRecordMultiplexer;
import communication.writers.FileRecordWriter;
import communication.writers.SHMRecordWriter;
import communication.writers.TCPRecordWriter;

import exceptions.InexistentApplicationException;
import execinfo.NodeGroup;
import execinfo.NodeMeasurements;
import execinfo.ResultSummary;

/**
 * This class is responsible for running a specific NodeGroup previously submitted to the Launcher.
 * 
 * @author Hammurabi Mendes (hmendes)
 * @author Marcelo Martins (martins)
 */
public class ExecutionHandler extends Thread {
	private Manager manager;

	private JobLauncher jobLauncher;

	private NodeGroup nodeGroup;

	static String registryLocation = System.getProperty("java.rmi.server.location");
	
	/**
	 * Constructor.
	 * 
	 * @param manager Reference to manager.
	 * @param jobLauncher Reference to local launcher.
	 * @param nodeGroup NodeGroup that should run.
	 */
	public ExecutionHandler(Manager manager, JobLauncher jobLauncher, NodeGroup nodeGroup) {
		this.manager = manager;

		this.jobLauncher = jobLauncher;

		StateManager groupManager = (StateManager) RMIHelper.locateRemoteObject(registryLocation, "GroupManager");
		StateManager stageManager = (StateManager) RMIHelper.locateRemoteObject(registryLocation, "StageManager");
		
		this.nodeGroup = nodeGroup;
		this.nodeGroup.setGroupManager(groupManager);
		this.nodeGroup.getStage().setStageManager(stageManager);

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
	@Override
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
	 *             If one of the following situations occur:
	 *             1) Error creating client-side or server-side TCP channels;
	 *             2) Address of server-side TCP channel required by NodeGroup 
	 *             is not registered in master;
	 *             3) Error creating or opening file channels.
	 */
	private void setupCommunication() throws Exception {
		/*
		 * Create all pipe handlers (readers and writers)
		 * If two pipe edges target the same node, only one pipe handler (and
		 * corresponding physical pipe) is created
		 */

		Map<String, SHMRecordMultiplexer> mapRecordOutputStream = new HashMap<String, SHMRecordMultiplexer>();

		for (Node node: nodeGroup.getNodes()) {
			SHMRecordMultiplexer shmRecordMultiplexer = null;

			for (InputChannel inputChannel: node.getInputChannels()) {
				if (inputChannel instanceof SHMInputChannel) {
					SHMInputChannel shmInputChannel = (SHMInputChannel) inputChannel;

					if (shmRecordMultiplexer == null) {
						shmRecordMultiplexer = new SHMRecordMultiplexer(node.getInputChannelNames());

						/*
						 * For SHM, when creating input pipe, map the
						 * associated output pipe for destination nodes
						 */
						mapRecordOutputStream.put(node.getName(), shmRecordMultiplexer);
					}

					// For SHM, all inputs come from the unique input pipe
					shmInputChannel.setRecordReader(shmRecordMultiplexer);
				}
			}
		}

		for (Node node: nodeGroup.getNodes()) {
			for (OutputChannel outputChannel: node.getOutputChannels()) {
				if (outputChannel instanceof SHMOutputChannel) {
					SHMOutputChannel shmOutputChannel = (SHMOutputChannel) outputChannel;

					/*
					 * For SHM, all the outputs go to the unique output pipe 
					 * for each node
					 */

					SHMRecordWriter shmRecordWriter = new SHMRecordWriter(node.getName(), mapRecordOutputStream.get(shmOutputChannel.getName()));

					shmOutputChannel.setRecordWriter(shmRecordWriter);
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

			for (InputChannel inputChannel: node.getInputChannels()) {
				if (inputChannel instanceof TCPInputChannel) {
					TCPInputChannel tcpInputChannel = (TCPInputChannel) inputChannel;

					if (tcpRecordMultiplexer == null) {
						tcpRecordMultiplexer = new TCPRecordMultiplexer(node.getInputChannelNames());

						tcpInputChannel.setSocketAddress(tcpRecordMultiplexer.getAddress());

						/* 
						 * For TCP, when creating the input server, map the
						 * associated output server addresses for other nodes
						 */
						boolean result = manager.registerSocketAddress(nodeGroup.getApplicationName(), node.getName(), tcpInputChannel.getSocketAddress());

						if (result == false) {
							System.err.println("Unable to insert socket address for application " + nodeGroup.getApplicationName() + " in manager!");

							throw new InexistentApplicationException(nodeGroup.getApplicationName());
						}
					}

					// For TCP, all inputs come from the unique input server
					tcpInputChannel.setRecordReader(tcpRecordMultiplexer);
				}
			}
		}

		for (Node node: nodeGroup.getNodes()) {
			for (OutputChannel outputChannel: node.getOutputChannels()) {
				if (outputChannel instanceof TCPOutputChannel) {
					TCPOutputChannel tcpOutputChannel = (TCPOutputChannel) outputChannel;

					/*
					 *  For TCP, (1) obtain the address of the output server
					 *  from manager
					 */
					InetSocketAddress socketAddress = manager.obtainSocketAddress(nodeGroup.getApplicationName(), tcpOutputChannel.getName());

					if(socketAddress == null) {
						throw new InexistentApplicationException(nodeGroup.getApplicationName());
					}

					tcpOutputChannel.setSocketAddress(socketAddress);

					/*
					 *  For TCP, (2) all the outputs go to the unique server 
					 *  for each node
					 */

					TCPRecordWriter tcpRecordWriter = new TCPRecordWriter(node.getName(), socketAddress);

					tcpOutputChannel.setRecordWriter(tcpRecordWriter);
				}
			}
		}

		/*
		 * Create all the file handlers
		 * If more than one file edge target the same node, more than one file
		 * handler (and corresponding file descriptor) is created
		 */

		for (Node node: nodeGroup.getNodes()) {
			for (InputChannel inputChannel: node.getInputChannels()) {
				if (inputChannel instanceof FileInputChannel) {
					FileInputChannel fileInputChannel = (FileInputChannel) inputChannel;

					FileRecordReader fileRecordReader = new FileRecordReader(fileInputChannel.getFilename());

					fileInputChannel.setRecordReader(fileRecordReader);
				}
			}

			for (OutputChannel outputChannel: node.getOutputChannels()) {
				if (outputChannel instanceof FileOutputChannel) {
					FileOutputChannel fileOutputChannel = (FileOutputChannel) outputChannel;

					FileRecordWriter fileRecordWriter = new FileRecordWriter(fileOutputChannel.getFilename());

					fileOutputChannel.setRecordWriter(fileRecordWriter);
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
		NodeProfileHandler[] nodeHandlers = new NodeProfileHandler[nodeGroup.getSize()];

		Iterator<Node> iterator = nodeGroup.getNodesIterator();

		long globalTimerStart = System.currentTimeMillis();

		for (int i = 0; i < nodeGroup.getSize(); i++) {
			nodeHandlers[i] = new NodeProfileHandler(iterator.next());

			nodeHandlers[i].start();
		}

		for (int i = 0; i < nodeGroup.getSize(); i++) {
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

		for (int i = 0; i < nodeGroup.getSize(); i++) {
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
		jobLauncher.delNodeGroup(nodeGroup);

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

		private long energyStart;
		private long energyFinish;
		
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
		@Override
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
			energyStart = node.getEnergy();

			node.run();

			realLocalTimerFinish = System.currentTimeMillis();

			cpuLocalTimerFinish = profiler.getCurrentThreadCpuTime();
			userLocalTimerFinish = profiler.getCurrentThreadUserTime();
			energyFinish = node.getEnergy();
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
		 * Getter for Node's energy expenditure
		 * 
		 * @return The energy spent to execute Node.
		 */
		public long getEnergy() {
			// Return energy in Joules
			
			return (energyFinish - energyStart);
		}
		/**
		 * Getter for the whole set of node measurements.
		 * 
		 * @return The whole set of node measurements.
		 */
		public NodeMeasurements getNodeMeasurements() {
			return new NodeMeasurements(getRealTime(), getCpuTime(), getUserTime(), getEnergy());
		}
	}
}
