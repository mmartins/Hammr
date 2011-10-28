package scheduler;

import java.rmi.RemoteException;

import java.util.Set;
import java.util.Map;
import java.util.Queue;

import java.util.HashSet;
import java.util.HashMap;
import java.util.LinkedList;

import org.jgrapht.alg.*;
import org.jgrapht.graph.*;

import execinfo.NodeGroup;
import execinfo.Stage;

import exceptions.InsufficientLaunchersException;
import exceptions.TemporalDependencyException;
import exceptions.CyclicDependencyException;

import appspecs.ApplicationSpecification;
import appspecs.Node;
import appspecs.Edge;
import appspecs.EdgeType;

import utilities.MutableInteger;

import interfaces.Launcher;

import manager.JobManager;

public class JobScheduler implements Scheduler {
	private JobManager concreteManager;
	private ApplicationSpecification applicationSpecification;

	// A Stage is only released when its NodeGroup dependencies have been executed
	
	private DependencyManager<NodeGroup, Stage> dependencyManager;

	// List of NodeGroups currently executing on Launchers
	
	private Map<Long, NodeGroup> scheduledNodeGroups;

	private long serialNumberCounter = 1L;

	/**
	 * Constructor method.
	 * 
	 * @param concreteManager Reference to the manager object.
	 */
	public JobScheduler(JobManager concreteManager) {
		this.concreteManager = concreteManager;
	}

	/**
	 * Specifies the application that this scheduler is responsible for.
	 * 
	 * @param applicationSpecification App spec this scheduler is responsible for.
	 */
	private void setApplicationSpecification(ApplicationSpecification applicationSpecification) {
		this.applicationSpecification = applicationSpecification;
	}

	/**
	 * Parses the graph and clusters Nodes that use shared memory as their communication primitive
	 * into NodeGroups. Each NodeGroup is assigned a serial number.
	 * 
	 * @return A list of NodeGroups indexed by their serial number.
	 */
	private Map<MutableInteger, NodeGroup> getNodeGroups() {
		Map<MutableInteger, NodeGroup> result = new HashMap<MutableInteger, NodeGroup>();

		Queue<Node> queue = new LinkedList<Node>();

		int currentSpammerIdentifier = 0;

		Node current, neighbor;

		for(Node spammer: applicationSpecification.vertexSet()) {
			if(spammer.isMarked()) {
				continue;
			}

			Set<Node> spammerGroup = new HashSet<Node>();

			MutableInteger spammerIdentifier = new MutableInteger(currentSpammerIdentifier++);

			spammer.setMark(spammerIdentifier);
			queue.add(spammer);

			while(queue.size() > 0) {
				current = queue.remove();

				spammerGroup.add(current);

				for(Edge connection: applicationSpecification.outgoingEdgesOf(current)) {
					neighbor = connection.getTarget();

					if(connection.getCommunicationMode() == EdgeType.SHM) {
						if(!neighbor.isMarked()) {
							neighbor.setMark(spammerIdentifier);
							queue.add(neighbor);
						}
					}
				}

				for(Edge connection: applicationSpecification.incomingEdgesOf(current)) {
					neighbor = connection.getSource();

					if(connection.getCommunicationMode() == EdgeType.SHM) {
						if(!neighbor.isMarked()) {
							neighbor.setMark(spammerIdentifier);
							queue.add(neighbor);
						}
					}
				}
			}

			result.put(spammerIdentifier, new NodeGroup(applicationSpecification.getName(), spammerGroup));
		}

		return result;
	}

	/**
	 * Parses the graph formed when we consider NodeGroups as single nodes and cluster NodeGroups that use TCP channels
	 * as their communication primitive into Stages. Each Stage is assigned a serial number.
	 * 
	 * @return A list of NodeGroups indexed by their serial number.
	 */
	private Map<MutableInteger, Stage> getStages() {
		Map<MutableInteger, NodeGroup> nodeGroups = getNodeGroups();

		DefaultDirectedGraph<NodeGroup, DefaultEdge> nodeGroupGraph = new DefaultDirectedGraph<NodeGroup, DefaultEdge>(DefaultEdge.class);

		for(NodeGroup nodeGroup: nodeGroups.values()) {
			nodeGroupGraph.addVertex(nodeGroup);
		}

		Node source, target;

		for(Edge edge: applicationSpecification.edgeSet()) {
			if(edge.getCommunicationMode() == EdgeType.TCP) {
				source = edge.getSource();
				target = edge.getTarget();

				nodeGroupGraph.addEdge(source.getNodeGroup(), target.getNodeGroup());
			}
		}

		Map<MutableInteger, Stage> result = new HashMap<MutableInteger, Stage>();

		Queue<NodeGroup> queue = new LinkedList<NodeGroup>();

		int currentBundleIdentifier = 0;

		NodeGroup current, neighbor;

		for(NodeGroup spammer: nodeGroupGraph.vertexSet()) {
			if(spammer.isMarked()) {
				continue;
			}

			Set<NodeGroup> spammerBundle = new HashSet<NodeGroup>();

			MutableInteger spammerIdentifier = new MutableInteger(currentBundleIdentifier++);

			spammer.setMark(spammerIdentifier);
			queue.add(spammer);

			while(queue.size() > 0) {
				current = queue.remove();

				spammerBundle.add(current);

				for(DefaultEdge connection: nodeGroupGraph.outgoingEdgesOf(current)) {
					neighbor = nodeGroupGraph.getEdgeTarget(connection);

					if(!neighbor.isMarked()) {
						neighbor.setMark(spammerIdentifier);
						queue.add(neighbor);
					}
				}

				for(DefaultEdge connection: nodeGroupGraph.incomingEdgesOf(current)) {
					neighbor = nodeGroupGraph.getEdgeSource(connection);

					if(!neighbor.isMarked()) {
						neighbor.setMark(spammerIdentifier);
						queue.add(neighbor);
					}
				}
			}

			result.put(spammerIdentifier, new Stage(spammerBundle));
		}

		return result;
	}

	// TODO: Insert the following functionality
	//       1) Verify whether all the initial node bundles are free (i.e., without dependencies)
	//       2) Add all the free dependencies into the dependency manager
	//       3) Guarantee that one file is read at most by one node
	/**
	 * Based on the Stages identified in the application specification, create dependencies that only release
	 * Stages when all their triggerer NodeGroups have their execution notified to the scheduler.
	 * @throws TemporalDependencyException
	 * @throws CyclicDependencyException
	 */
	private void createStageDependencies() throws TemporalDependencyException, CyclicDependencyException {
		Map<MutableInteger, Stage> stages = getStages();

		System.out.println("Identified node group bundles:");

		for(Stage x: stages.values()) {
			System.out.println(x);
		}

		DefaultDirectedGraph<Stage, DefaultEdge> stageGraph = new DefaultDirectedGraph<Stage, DefaultEdge>(DefaultEdge.class);

		for(Stage stage: stages.values()) {
			stageGraph.addVertex(stage);
		}

		Node source, target;

		for(Edge edge: applicationSpecification.edgeSet()) {
			if(edge.getCommunicationMode() == EdgeType.FILE) {
				source = edge.getSource();
				target = edge.getTarget();

				stageGraph.addEdge(source.getNodeGroup().getStage(), target.getNodeGroup().getStage());
			}
		}

		CycleDetector<Stage, DefaultEdge> cycleDetector = new CycleDetector<Stage, DefaultEdge>(stageGraph);

		if(cycleDetector.detectCycles()) {
			throw new CyclicDependencyException();
		}

		for(Node node: applicationSpecification.getSourceNodes()) {
			dependencyManager.insertDependency(null, node.getNodeGroup().getStage());
		}

		for(Edge edge: applicationSpecification.edgeSet()) {
			if(edge.getCommunicationMode() == EdgeType.FILE) {
				source = edge.getSource();
				target = edge.getTarget();

				if(source.getNodeGroup().getStage() == target.getNodeGroup().getStage()) {
					throw new TemporalDependencyException(source, target);
				}

				dependencyManager.insertDependency(source.getNodeGroup(), target.getNodeGroup().getStage());
			}
		}
	}

	/**
	 * Try to schedule the next wave of NodeGroups: Stages are NodeGroups that should
	 * be schedule at the same time.
	 * 
	 * @return False if no Stage is available to execution; true otherwise.
	 * 
	 * @throws InsufficientLaunchersException If no alive Launcher can receive the next wave of NodeGroups.
	 */
	public synchronized boolean scheduleStage() throws InsufficientLaunchersException {
		if(!dependencyManager.hasFreeDependents()) {
			return false;
		}

		Set<Stage> freeStages = dependencyManager.obtainFreeDependents();

		for(Stage freeStage: freeStages) {
			System.out.println("Scheduling node bundle " + freeStage);

			for(NodeGroup nodeGroup: freeStage) {
				scheduleNodeGroup(nodeGroup);
			}
		}

		return true;
	}

	/**
	 * Try to schedule the informed NodeGroup.
	 * 
	 * @throws InsufficientLaunchersException If no alive Launcher can receive the next wave of NodeGroups.
	 */
	private void scheduleNodeGroup(NodeGroup nodeGroup) throws InsufficientLaunchersException {
		nodeGroup.prepareSchedule(serialNumberCounter++);

		while(true) {
			try {
				Launcher launcher = getRandomLauncher();

				scheduledNodeGroups.put(nodeGroup.getSerialNumber(), nodeGroup);

				launcher.addNodeGroup(nodeGroup);

				break;
			} catch (RemoteException exception) {
				System.err.println("Failed using launcher, trying next one...");

				scheduledNodeGroups.remove(nodeGroup.getSerialNumber());

				exception.printStackTrace();
			}
		}	
	}

	/**
	 * Obtains a random alive Launcher from the Manager.
	 * 
	 * @return A random alive Launcher.
	 * 
	 * @throws InsufficientLaunchersException If there are no alive Launchers.
	 */
	private Launcher getRandomLauncher() throws InsufficientLaunchersException {
		Launcher launcher =  concreteManager.getRandomLauncher();

		if(launcher == null) {
			throw new InsufficientLaunchersException();
		}

		return launcher;
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
		NodeGroup terminated = scheduledNodeGroups.remove(serialNumber);

		if(terminated != null) {
			dependencyManager.removeDependency(terminated);

			return true;
		}

		return false;
	}

	/**
	 * Setups the scheduler for the new application being executed.
	 * @param applicationSpecification Application specification.
	 * 
	 * @return True if the setup finished successfully; false otherwise.
	 * 
	 * @throws TemporalDependencyException If the application specification has a temporal dependency problem.
	 * @throws CyclicDependencyException If the application specification has a cyclic dependency problem.
	 */
	public synchronized boolean setup(ApplicationSpecification applicationSpecification) throws TemporalDependencyException, CyclicDependencyException {
		setApplicationSpecification(applicationSpecification);

		dependencyManager = new DependencyManager<NodeGroup, Stage>();

		scheduledNodeGroups = new HashMap<Long, NodeGroup>();

		long graphParsingStartTimer = System.currentTimeMillis();

		createStageDependencies();

		long graphParsingEndingTimer = System.currentTimeMillis();

		System.out.println("Time to parse graph for application " + applicationSpecification.getName() + ": " + (graphParsingEndingTimer - graphParsingStartTimer) + " msec");

		return true;
	}

	/**
	 * Tests whether all the Node/NodeGroups were already executed.
	 * 
	 * @return True if all the Node/NodeGroups were already executed, false otherwise.
	 */
	public synchronized boolean finished() {
		return (!dependencyManager.hasLockedDependents() && !dependencyManager.hasFreeDependents() && (scheduledNodeGroups.size() == 0));
	}
}
