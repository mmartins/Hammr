/*

Copyright (c) 2011, Marcelo Martins

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.  Redistributions in binary
form must reproduce the above copyright notice, this list of conditions and the
following disclaimer in the documentation and/or other materials provided with
the distribution.  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.

*/

package manager;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import execinfo.NodeGroup;
import execinfo.ProgressReport;
import interfaces.StateManager;
import utilities.RMIHelper;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

/**
 * Concrete implementation of Manager.
 * 
 * @author Marcelo Martins (martins)
 */
public class GroupPowerManager implements StateManager {
	
	private static GroupPowerManager instance;

	private String baseDirectory;

	// Group performance policy
	private int policy;
	
	// Policy thresholds
	private double maxThreshold;
	
	private double minThreshold;
	
	// Active groups, mapped by stage ID
	private Map<Long, DescriptiveStatistics> registeredGroups;
	
	// Active reports, mapped by group ID
	private Map<NodeGroup, ProgressReport> registeredReports;

	static {
		String registryLocation = System.getProperty("java.rmi.server.location");
		String baseDirectory = System.getProperty("hammr.group_manager.basedir");

		instance = setupManager(registryLocation, baseDirectory);
	}

	/**
	 * Setups a manager for execution.
	 * 
	 * @param registryLocation Location of the registry used to store the manager reference.
	 * 
	 * @return A manager ready for execution.
	 */
	private static GroupPowerManager setupManager(String registryLocation, String baseDirectory) {
		// Initiates a stage manager

		GroupPowerManager manager = new GroupPowerManager(baseDirectory);

		// Makes the manager available for remote calls

		RMIHelper.exportAndRegisterRemoteObject(registryLocation, "GroupManager", manager);

		return manager;
	}

	/**
	 * Notifies manager of new group start. Called by Launchers.
	 * 
	 * @param group	Started group.
	 * 
	 * @return True unless group is not reachable.
	 */
	public boolean registerStateHolder(Object holder) {
		NodeGroup group = (NodeGroup) holder;

		/* Only holders belonging to registered applications can register */
		if (registeredGroups.containsKey(group.getStage().getSerialNumber()) == false)
			return false;

		registeredReports.put(group, null);
		
		System.out.println("Registered group with ID " + group.getSerialNumber());

		return true;
	}

	/**
	 * Return the singleton instance of the stage manager.
	 * 
	 * @return The singleton instance of the stage manager.
	 */
	public static StateManager getInstance() {
		return instance;
	}
	
	public String getBaseDirectory() {
		return baseDirectory;
	}

	public void setPolicy(int policy) {
		this.policy = policy;
	}
	
	public int getPolicy() {
		return policy;
	}
	
	public void setMinThreshold(double minThreshold) {
		this.minThreshold = minThreshold;
	}
	
	public double getMinThreshold() {
		return minThreshold;
	}
	
	public void setMaxThreshold(double maxThreshold) {
		this.maxThreshold = maxThreshold;
	}
	
	public double getMaxThreshold() {
		return maxThreshold;
	}
	
	/**
	 * Returns the list of registered stages IDs.
	 * 
	 * @return The list of registered stages IDs
	 */
	public Collection<Long> getRegisteredStages() {
		return registeredGroups.keySet();
	}
	
	/**
	 * Returns the list of registered groups.
	 * 
	 * @return The list of registered groups.
	 */
	public Collection<NodeGroup> getRegisteredGroups() {
		return registeredReports.keySet();
	}
	
	/**
	 * Constructor method.
	 * 
	 * @param baseDirectory Working directory of the manager.
	 */
	public GroupPowerManager(String baseDirectory) {
		this.registeredGroups = Collections.synchronizedMap(new HashMap<Long, DescriptiveStatistics>());
		this.registeredReports = Collections.synchronizedMap(new HashMap<NodeGroup, ProgressReport>());
		this.baseDirectory = baseDirectory;
	}

	public boolean receiveState(Object stateHolder, Object state) throws RemoteException {
		ProgressReport prevReport, report = (ProgressReport) state;
		NodeGroup group = (NodeGroup) stateHolder;
		long stageId = group.getStage().getSerialNumber();
		DescriptiveStatistics stats;
		
		/* Only holders belonging to registered applications can report state */
		if (registeredGroups.containsKey(stageId) == false)
			return false;

		prevReport = registeredReports.get(group);
		
		if (prevReport == null)
			return false;
		
		if (prevReport.getProgress() <= report.getProgress()) {
			return false;
		}
		
		registeredReports.put(group, report);
		stats = registeredGroups.get(stageId);
		stats.addValue(report.getProgress());
		
		/* Only proceed with policy once all groups belonging to stage have reported their progress */
		if (stats.getN() < registeredReports.size())
			return true;
		
		double mean = stats.getMean();
		
		switch (policy) {
			case GroupPolicy.ENERGY:
				for (Map.Entry<NodeGroup, ProgressReport> entry : registeredReports.entrySet()) {
					if (entry.getValue().getProgress() - mean > maxThreshold) {
						entry.getKey().reducePerformance();
					}
				}
				
				break;
				
			case GroupPolicy.PERFORMANCE:

				for (Map.Entry<NodeGroup, ProgressReport> entry : registeredReports.entrySet()) {
					
					/* If task is finished, reduce frequency to minimum */
					if (entry.getValue().getProgress() == 1.0) {
						entry.getKey().reducePerformance();
					}
					/* Else, force group to run at max frequency */
					else if (mean - entry.getValue().getProgress() > maxThreshold) {
						entry.getKey().increasePerformance();
					}
				}

				break;
				
			case GroupPolicy.MODERATE:
				for (Map.Entry<NodeGroup, ProgressReport> entry : registeredReports.entrySet()) {

					/* If task is finished, reduce frequency to minimum */
					if (entry.getValue().getProgress() == 1.0) {
						entry.getKey().reducePerformance();
					}
					
					/* Else, try to balance */
					else {
						if (entry.getValue().getProgress() - mean > maxThreshold) {
							entry.getKey().reducePerformance();
						}
					
						if (mean - entry.getValue().getProgress() > maxThreshold) {
							entry.getKey().increasePerformance();
						}
					}
				}

				break;
		}
		
		// Clear report map
		for (NodeGroup g : registeredReports.keySet()) {			
			registeredReports.put(g, null);
		}
		
		registeredGroups.get(stageId).clear();
		
		return true;
	}
	
	/**
	 * Notifies master that a NodeGroup has finished execution. Called by Launchers.
	 * 
	 * @param resultSummary	Summary containing NodeGroup's runtime information.
	 * 
	 * @return True if scheduling works as expected; false otherwise.
	 */
	public boolean handleTermination() {	
		return true;
	}

	/**
	 * Override basic toString()
	 */
	public String toString() {
		return "Group power manager running on directory \"" + baseDirectory + "\"";
	}

	/**
	 * Manager startup method.
	 * 
	 * @param arguments A list containing:
	 *        1) The registry location;
	 *        2) The manager working directory.
	 */
	public static void main(String[] arguments) {
		System.out.println("Running " + GroupPowerManager.getInstance().toString());
	}
	
	public static class GroupPolicy {
		public static final int PERFORMANCE = 0;
		public static final int ENERGY = 1;
		public static final int MODERATE = 2;
	}
}