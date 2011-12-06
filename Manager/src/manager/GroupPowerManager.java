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
import utilities.RMIHelper;

/**
 * Concrete implementation of Manager.
 * 
 * @author Marcelo Martins (martins)
 */
public class GroupPowerManager implements StateManager {
	
	private static GroupPowerManager instance;

	private String baseDirectory;

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
	 * Return the singleton instance of the stage manager.
	 * 
	 * @return The singleton instance of the stage manager.
	 */
	public static StateManager getInstance() {
		return instance;
	}

	/**
	 * Constructor method.
	 * 
	 * @param baseDirectory Working directory of the manager.
	 */
	public GroupPowerManager(String baseDirectory) {
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
}