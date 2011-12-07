/*
Copyright (c) 2011, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package scheduler;

import interfaces.Aggregator;
import interfaces.Launcher;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import manager.JobManager;

import org.jgrapht.alg.CycleDetector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import utilities.MutableInteger;
import utilities.RMIHelper;
import utilities.filesystem.FileHelper;
import utilities.filesystem.Filename;
import appspecs.ApplicationSpecification;
import appspecs.Decider;
import appspecs.Edge;
import appspecs.Node;
import enums.CommunicationMode;
import exceptions.CyclicDependencyException;
import exceptions.InexistentInputException;
import exceptions.InexistentOutputException;
import exceptions.InsufficientLaunchersException;
import exceptions.TemporalDependencyException;
import execinfo.NodeGroup;
import execinfo.Stage;

public class JobScheduler implements Scheduler {
	
	private String applicationName;
	private ApplicationSpecification applicationSpecification;

	/////////////////////////
	// PARSING INFORMATION //
	/////////////////////////

	Map<MutableInteger, NodeGroup> nodeGroups;
	Map<MutableInteger, Stage> stages;

	// A Stage is only released when its NodeGroup dependencies are executed

	private DependencyManager<NodeGroup, Stage> dependencyManager;

	// List of NodeGroups prepared, running, and terminated

	private Map<Long, NodeGroup> runningNodeGroups;

	private long serialNumberCounter = 1L;

	/**
	 * Constructor method.
	 * 
	 * @param applicationName The name of the application this scheduler works on.
	 */
	public JobScheduler(String applicationName) {
		this.applicationName = applicationName;
	}

	/**
	 * Setups scheduler for new application
	 * 
	 * @throws TemporalDependencyException If the application specification has a temporal dependency problem.
	 * @throws CyclicDependencyException If the application specification has a cyclic dependency problem.
	 *                                   A cyclic is only a problem when it involves files (TCP/SHM cycles are allowed).
	 */
	public synchronized void prepareApplication() throws TemporalDependencyException, CyclicDependencyException {
		// Initiate data structures

		this.applicationSpecification = JobManager.getInstance().getApplicationPackage(applicationName).getApplicationSpecification();

		this.dependencyManager = new DependencyManager<NodeGroup, Stage>();

		this.runningNodeGroups = new HashMap<Long, NodeGroup>();

		// Parse the application graph

		long graphParsingStartTimer = System.currentTimeMillis();

		Map<MutableInteger, Stage> stages = getStages();

		long graphParsingEndingTimer = System.currentTimeMillis();

		System.out.println("Time to parse graph for application " + applicationSpecification.getName() + ": " + (graphParsingEndingTimer - graphParsingStartTimer) + " msec");

		// Display identified stages

		System.out.println("Identified stages:");

		for (MutableInteger x: stages.keySet()) {
			Stage s = stages.get(x);
			s.setSerialNumber(x.getValue());
			System.out.println(s);
		}

		// Detect cyclic dependency problems

		Node source, target;

		DefaultDirectedGraph<Stage, DefaultEdge> stageGraph = new DefaultDirectedGraph<Stage, DefaultEdge>(DefaultEdge.class);

		for (Stage stage: stages.values()) {
			stageGraph.addVertex(stage);
		}

		for (Edge edge: applicationSpecification.edgeSet()) {
			if (edge.getCommunicationMode() == CommunicationMode.FILE) {
				source = edge.getSource();
				target = edge.getTarget();

				stageGraph.addEdge(source.getNodeGroup().getStage(), target.getNodeGroup().getStage());
			}
		}

		CycleDetector<Stage, DefaultEdge> cycleDetector = new CycleDetector<Stage, DefaultEdge>(stageGraph);

		if (cycleDetector.detectCycles()) {
			throw new CyclicDependencyException();
		}

		// Detect temporal dependency problems

		for (Edge edge: applicationSpecification.edgeSet()) {
			if (edge.getCommunicationMode() == CommunicationMode.FILE) {
				source = edge.getSource();
				target = edge.getTarget();

				if(source.getNodeGroup().getStage() == target.getNodeGroup().getStage()) {
					throw new TemporalDependencyException(source, target);
				}
			}
		}

		// Exports all the aggregators

		for(String variable: applicationSpecification.getAggregators().keySet()) {
			Aggregator<? extends Serializable,? extends Serializable> aggregator = applicationSpecification.getAggregator(variable);

			RMIHelper.exportRemoteObject(aggregator);
		}
	}

	/**
	 * Parses the graph and clusters Nodes that use shared memory as their communication primitive
	 * into NodeGroups. Each NodeGroup is assigned a serial number.
	 * 
	 * @return A list of NodeGroups indexed by their serial number.
	 */
	private Map<MutableInteger, NodeGroup> getNodeGroups() {
		if(nodeGroups != null) {
			return nodeGroups;
		}

		Map<MutableInteger, NodeGroup> result = new HashMap<MutableInteger, NodeGroup>();

		Queue<Node> queue = new LinkedList<Node>();

		int currentSpammerIdentifier = 0;

		Node current, neighbor;

		for (Node spammer: applicationSpecification.vertexSet()) {
			if (spammer.isMarked()) {
				continue;
			}

			Set<Node> spammerGroup = new HashSet<Node>();

			MutableInteger spammerIdentifier = new MutableInteger(currentSpammerIdentifier++);

			spammer.setMark(spammerIdentifier);
			queue.add(spammer);

			while (queue.size() > 0) {
				current = queue.remove();

				spammerGroup.add(current);

				for (Edge connection: applicationSpecification.outgoingEdgesOf(current)) {
					neighbor = connection.getTarget();

					if (connection.getCommunicationMode() == CommunicationMode.SHM) {
						if (!neighbor.isMarked()) {
							neighbor.setMark(spammerIdentifier);
							queue.add(neighbor);
						}
					}
				}

				for (Edge connection: applicationSpecification.incomingEdgesOf(current)) {
					neighbor = connection.getSource();

					if (connection.getCommunicationMode() == CommunicationMode.SHM) {
						if (!neighbor.isMarked()) {
							neighbor.setMark(spammerIdentifier);
							queue.add(neighbor);
						}
					}
				}
			}

			result.put(spammerIdentifier, new NodeGroup(applicationSpecification.getName(), spammerGroup));
		}

		nodeGroups = result;
		return result;
	}

	/**
	 * Parses the graph formed when we consider NodeGroups as single nodes and cluster NodeGroups that use TCP channels
	 * as their communication primitive into Stages. Each Stage is assigned a serial number.
	 * 
	 * @return A list of NodeGroups indexed by their serial number.
	 */
	private Map<MutableInteger, Stage> getStages() {
		if (stages != null) {
			return stages;
		}

		Map<MutableInteger, NodeGroup> nodeGroups = getNodeGroups();

		DefaultDirectedGraph<NodeGroup, DefaultEdge> nodeGroupGraph = new DefaultDirectedGraph<NodeGroup, DefaultEdge>(DefaultEdge.class);

		for (NodeGroup nodeGroup: nodeGroups.values()) {
			nodeGroupGraph.addVertex(nodeGroup);
		}

		Node source, target;

		for (Edge edge: applicationSpecification.edgeSet()) {
			if (edge.getCommunicationMode() == CommunicationMode.TCP) {
				source = edge.getSource();
				target = edge.getTarget();

				nodeGroupGraph.addEdge(source.getNodeGroup(), target.getNodeGroup());
			}
		}

		Map<MutableInteger, Stage> result = new HashMap<MutableInteger, Stage>();

		Queue<NodeGroup> queue = new LinkedList<NodeGroup>();

		int currentBundleIdentifier = 0;

		NodeGroup current, neighbor;

		for (NodeGroup spammer: nodeGroupGraph.vertexSet()) {
			if (spammer.isMarked()) {
				continue;
			}

			Set<NodeGroup> spammerBundle = new HashSet<NodeGroup>();

			MutableInteger spammerIdentifier = new MutableInteger(currentBundleIdentifier++);

			spammer.setMark(spammerIdentifier);
			queue.add(spammer);

			while (queue.size() > 0) {
				current = queue.remove();

				spammerBundle.add(current);

				for (DefaultEdge connection: nodeGroupGraph.outgoingEdgesOf(current)) {
					neighbor = nodeGroupGraph.getEdgeTarget(connection);

					if (!neighbor.isMarked()) {
						neighbor.setMark(spammerIdentifier);
						queue.add(neighbor);
					}
				}

				for (DefaultEdge connection: nodeGroupGraph.incomingEdgesOf(current)) {
					neighbor = nodeGroupGraph.getEdgeSource(connection);

					if (!neighbor.isMarked()) {
						neighbor.setMark(spammerIdentifier);
						queue.add(neighbor);
					}
				}
			}

			result.put(spammerIdentifier, new Stage(spammerBundle));
		}

		stages = result;
		return result;
	}
	
	/**
	 * Terminates the application .
	 */
	public synchronized void terminateApplication() {
		// Do nothing special
	}

	/**
	 * Tests whether the application has finished.
	 * 
	 * @return True if the application has finished, false otherwise.
	 */
	public synchronized boolean finishedApplication() {
		Decider decider = applicationSpecification.getDecider();

		Map<String, Aggregator<? extends Serializable,? extends Serializable>> aggregators = applicationSpecification.getAggregators();

		if(decider == null) {
			return true;
		}

		// Run the graph transformation in the decider, and possibly
		// make new processes available to execute.

		decider.decideFollowingIteration(aggregators);

		// Returns true if the decider prepared a following iteration

		return decider.requiresRunning();
	}

	/**
	 * Prepare an iteration for the application.
	 * 
	 * @throws InexistentInputException If one of the inputs are missing.
	 */
	public synchronized void prepareIteration() throws InexistentInputException {
		// Check if all the inputs are present

		List<Filename> missingInputs = new ArrayList<Filename>();;

		for(Filename input: applicationSpecification.getInputFilenames()) {
			if(!FileHelper.exists(input)) {
				missingInputs.add(input);
			}
		}

		if(missingInputs.size() != 0) {
			throw new InexistentInputException(missingInputs);
		}

		// Find out the initial nodes:
		// - If the application specification defines the intials, use them
		// - Otherwise get file consumers that only depend on system files

		Node source, target;

		Set<Node> initials = applicationSpecification.getInitials();

		if(initials.size() == 0) {
			initials = new HashSet<Node>(applicationSpecification.getFileConsumers());

			for(Edge edge: applicationSpecification.edgeSet()) {
				target = edge.getTarget();

				if(initials.contains(target)) {
					initials.remove(target);
				}
			}
		}

		// Notify the dependency manager that the initial nodes should be immediately available to schedule

		for(Node initial: initials) {
			dependencyManager.insertDependency(null, initial.getNodeGroup().getStage());
		}

		// Notify the other dependencies for the dependency manager

		for(Edge edge: applicationSpecification.edgeSet()) {
			if(edge.getCommunicationMode() == CommunicationMode.FILE) {
				source = edge.getSource();
				target = edge.getTarget();

				dependencyManager.insertDependency(source.getNodeGroup(), target.getNodeGroup().getStage());
			}
		}

		// Prepare all nodes and node groups for scheduling

		for(NodeGroup nodeGroup: nodeGroups.values()) {
			nodeGroup.prepareSchedule(serialNumberCounter++);

			for(Node node: nodeGroup.getNodes()) {
				node.prepareSchedule();
			}
		}
	}
	
	/**
	 * Terminates the iteration .
	 * 
	 * @throws InexistentOutputException If one of the outputs are missing.
	 */
	public synchronized void terminateIteration() throws InexistentOutputException {
		// If the iteration is finished, check if all the outputs are present

		List<Filename> missingOutputs = new ArrayList<Filename>();

		for(Filename output: applicationSpecification.getOutputFilenames()) {
			if(!FileHelper.exists(output)) {
				missingOutputs.add(output);
			}
		}

		if(missingOutputs.size() != 0) {
			throw new InexistentOutputException(missingOutputs);
		}
	}

	/**
	 * Tests whether all the Node/NodeGroups were already executed for this iteration.
	 * 
	 * @return True if all the Node/NodeGroups were already executed for this iteration, false otherwise.
	 */
	public synchronized boolean finishedIteration() {
		return (!dependencyManager.hasLockedDependents() && !dependencyManager.hasUnlockedDependents() && (runningNodeGroups.size() == 0));
	}

	/**
	 * Try to schedule the next wave of NodeGroups: Stages are NodeGroups that should
	 * be schedule at the same time.
	 * 
	 * @return False if no Stage is available to execution; true otherwise.
	 * 
	 * @throws InsufficientLaunchersException If no alive Launcher can receive the next wave of NodeGroups.
	 */
	public synchronized boolean schedule() throws InsufficientLaunchersException {
		if (!dependencyManager.hasUnlockedDependents()) {
			return false;
		}

		Set<Stage> freeStages = dependencyManager.obtainFreeDependents();

		for (Stage freeStage: freeStages) {
			System.out.println("Scheduling node bundle " + freeStage);

			scheduleStage(freeStage);
		}

		return true;
	}

	/**
	 * Try to schedule the informed Stage.
	 * 
	 * @throws InsufficientLaunchersException If no alive Launcher can receive NodeGroups.
	 */
	private void scheduleStage(Stage stage) throws InsufficientLaunchersException {
		for(NodeGroup nodeGroup: stage.getNodeGroups()) {
			scheduleNodeGroup(nodeGroup);
		}
	}

	/**
	 * Try to schedule the informed NodeGroup.
	 * 
	 * @throws InsufficientLaunchersException If no alive Launcher can receive the next wave of NodeGroups.
	 */
	private void scheduleNodeGroup(NodeGroup nodeGroup) throws InsufficientLaunchersException {
		// First, try to reschedule the node group to the same launcher used before

		Launcher previousLauncher = nodeGroup.getPreviousLauncher();

		if (previousLauncher != null) {
			try {
				if(previousLauncher.addNodeGroup(nodeGroup)) {
					// Add node group to the running group
					runningNodeGroups.put(nodeGroup.getSerialNumber(), nodeGroup);

					return;
				}
			} catch (RemoteException exception) {
				System.err.println("Previous launcher for NodeGroup #" + nodeGroup.getSerialNumber() + " is no longer running. Trying a differnt one...");
			}
		}

		List<Launcher> currentLaunchers = new ArrayList<Launcher>(JobManager.getInstance().getRegisteredLaunchers());

		Collections.shuffle(currentLaunchers);

		for (int i = 0; i < currentLaunchers.size(); i++) {
			try {
				Launcher launcher = currentLaunchers.get(i);

				if (launcher.addNodeGroup(nodeGroup)) {
					// Remember this scheduling decision
					nodeGroup.setPreviousLauncher(launcher);

					// Add node group to the running group
					runningNodeGroups.put(nodeGroup.getSerialNumber(), nodeGroup);

					return;
				}
				else {
					System.err.println("Failed using launcher (launcher unusable), trying next one...");
				}
			} catch (RemoteException exception) {
				System.err.println("Failed using launcher (launcher unreachable), trying next one...");
			}
		}	

		throw new InsufficientLaunchersException();
	}

	/**
	 * Informs the scheduler a particular NodeGroup has finished its execution.
	 * 
	 * @param serialNumber The serial number of the NodeGroup that has finished its execution.
	 * 
	 * @return True if this is the first termination notification for this NodeGroup; false otherwise. The current
	 * scheduler implementation only has one possible termination notification, since it doesn't handle failures.
	 */
	public synchronized boolean handleTermination(Long serialNumber) {
		NodeGroup terminated = runningNodeGroups.remove(serialNumber);

		if (terminated != null) {
			dependencyManager.removeDependency(terminated);

			return true;
		}

		return false;
	}
}
