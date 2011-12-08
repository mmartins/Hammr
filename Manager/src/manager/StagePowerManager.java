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

import interfaces.StateManager;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import utilities.RMIHelper;
import execinfo.Stage;

/**
 * Concrete implementation of Manager.
 * 
 * @author Marcelo Martins (martins)
 */
public class StagePowerManager implements StateManager {
	
	private static StagePowerManager instance;

	private String baseDirectory;

	// Active groups, mapped by ID
	private Map<Long, Stage> registeredStages;

	static {
		String registryLocation = System.getProperty("java.rmi.server.location");
		String baseDirectory = System.getProperty("hammr.stage_manager.basedir");

		instance = setupManager(registryLocation, baseDirectory);
	}

	/**
	 * Setups a manager for execution.
	 * 
	 * @param registryLocation Location of the registry used to store the manager reference.
	 * 
	 * @return A manager ready for execution.
	 */
	private static StagePowerManager setupManager(String registryLocation, String baseDirectory) {
		// Initiates a stage manager

		StagePowerManager manager = new StagePowerManager(baseDirectory);

		// Makes the manager available for remote calls

		RMIHelper.exportAndRegisterRemoteObject(registryLocation, "StageManager", manager);

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
		Stage stage = (Stage) holder;
		long stageId;

		stageId = stage.getSerialNumber();

		registeredStages.put(stageId, stage);
		System.out.println("Registered stage with ID " + stageId);

		return true;
	}

	/**
	 * Reports new state to state manager. Called by State holders.
	 * 
	 * @param stateHolder	state holder.
	 * @param state			state.
	 * 
	 * @return True unless holder is not reachable.
	 */
	public boolean receiveState(Object stateHolder, Object state) throws RemoteException {
		/* TODO: Fill this */
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

	/**
	 * Returns the list of registered groups.
	 * 
	 * @return The list of registered groups.
	 */
	public Collection<Stage> getRegisteredStages() {
		return registeredStages.values();
	}

	/**
	 * Constructor method.
	 * 
	 * @param baseDirectory Working directory of the manager.
	 */
	public StagePowerManager(String baseDirectory) {
		this.registeredStages = Collections.synchronizedMap(new HashMap<Long, Stage>());
		this.baseDirectory = baseDirectory;
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
		return "Stage power manager running on directory \"" + baseDirectory + "\"";
	}

	/**
	 * Manager startup method.
	 * 
	 * @param arguments A list containing:
	 *        1) The registry location;
	 *        2) The manager working directory.
	 */
	public static void main(String[] arguments) {
		System.out.println("Running " + StagePowerManager.getInstance().toString());
	}
}
