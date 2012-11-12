/*
Copyright (c) 2010, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package manager;

import interfaces.ApplicationAggregator;
import interfaces.ApplicationController;
import interfaces.Launcher;
import interfaces.Manager;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import scheduler.JobScheduler;
import scheduler.Scheduler;
import utilities.RMIHelper;
import appspecs.ApplicationSpecification;
import exceptions.CyclicDependencyException;
import exceptions.InexistentInputException;
import exceptions.InexistentOutputException;
import exceptions.InsufficientLaunchersException;
import exceptions.TemporalDependencyException;
import execinfo.ResultSummary;

/**
 * Concrete implementation of Manager.
 * 
 * @author Hammurabi Mendes (hmendes)
 * @author Marcelo Martins (martins)
 */
public class JobManager implements Manager {
	
	private static JobManager instance;

	private String baseDirectory;

	// Active launchers, mapped by ID
	private Map<String, Launcher> registeredLaunchers;

	// Active applications, mapped by name
	private Map<String, ApplicationPackage> applicationPackages;

	static {
		String registryLocation = System.getProperty("java.rmi.server.location");
		String baseDirectory = System.getProperty("hammr.manager.basedir");

		instance = setupManager(registryLocation, baseDirectory);
	}

	/**
	 * Setups a manager for execution.
	 * 
	 * @param registryLocation Location of the registry used to store the manager reference.
	 * 
	 * @return A manager ready for execution.
	 */
	private static JobManager setupManager(String registryLocation, String baseDirectory) {
		// Initiates a concrete manager

		JobManager manager = new JobManager(baseDirectory);

		// Makes the manager available for remote calls

		RMIHelper.exportAndRegisterRemoteObject(registryLocation, "Manager", manager);

		return manager;
	}

	/**
	 * Return the singleton instance of the manager.
	 * 
	 * @return The singleton instance of the manager.
	 */
	public static JobManager getInstance() {
		return instance;
	}

	/**
	 * Constructor method.
	 * 
	 * @param baseDirectory Working directory of the manager.
	 */
	public JobManager(String baseDirectory) {
		this.baseDirectory = baseDirectory;

		this.registeredLaunchers = Collections.synchronizedMap(new LinkedHashMap<String, Launcher>());

		this.applicationPackages = Collections.synchronizedMap(new HashMap<String, ApplicationPackage>());
	}

	/**
	 * Notifies manager of new launcher start. Called by Launchers.
	 * 
	 * @param launcher	Started launcher.
	 * 
	 * @return True unless launcher is not reachable.
	 */
	public synchronized boolean registerLauncher(Launcher launcher) {
		String launcherId;

		try {
			launcherId = launcher.getId();

			registeredLaunchers.put(launcherId, launcher);
			System.out.println("Registered launcher with ID " + launcherId);

			return true;
		} catch (RemoteException exception) {
			System.err.println("Unable to get ID for launcher: " + exception.toString());
			exception.printStackTrace();

			return false;
		}
	}

	/**
	 * Submits a new application for scheduling. Called by clients.
	 * 
	 * @param applicationSpecification Specification of to-be-run application
	 * 
	 * @return True unless:
	 *         1) Application with the same name is already running;
	 *         2) Scheduler setup for application went wrong;
	 *         
	 */
	public synchronized boolean registerApplication(ApplicationSpecification applicationSpecification) {
		String applicationName = applicationSpecification.getName();

		// Trying to register an applicationName that's still running

		if (applicationPackages.containsKey(applicationName)) {
			System.err.println("Application " + applicationName + " is already running!");

			return false;
		}

		// Setup the scheduler, and try to schedule an initial wave of NodeGroups

		try {
			ApplicationPackage applicationPackage = setupApplication(applicationName, applicationSpecification);
			
			Scheduler scheduler = applicationPackage.getApplicationScheduler();
			
			if (!scheduler.schedule()) {
				System.err.println("Initial schedule indicated that no free node group bundles are present");
				
				finishApplication(applicationName);
				return false;
			}
		} catch (TemporalDependencyException exception) {
			System.err.println("Scheduler setup found a temporal dependency problem");

			finishApplication(applicationName);
			return false;
		} catch (CyclicDependencyException exception) {
			System.err.println("Scheduler setup found a cyclic dependency problem");

			finishApplication(applicationName);
			return false;
		} catch (InsufficientLaunchersException exception) {
			System.err.println("Initial schedule indicated an insufficient number of launchers");

			finishApplication(applicationName);
			return false;
		} catch (InexistentInputException exception) {
			System.err.println("Initial schedule indicated that some files are missing: " + exception.toString());

			finishApplication(applicationName);
			return false;
		}
		
		return true;
	}

	/**
	 * Registers server-side TCP channel socket address with manager. Called
	 * within Laucher during setup of NodeGroups that have server-side TCP
	 * channels. The corresponding client-side TCP channels query master for
	 * this information.
	 * 
	 * @param applicationName	Ditto.
	 * @param nodeName			Ditto.
	 * @param socketAddress		Socket address of server-side TCP channel.
	 * 
	 * @return True unless the map for the specific pair applicationName/node
	 *         already exists.
	 */
	public boolean registerSocketAddress(String applicationName, String nodeName, InetSocketAddress socketAddress) throws RemoteException {
		ApplicationPackage applicationPackage = applicationPackages.get(applicationName);

		if (applicationPackage == null) {
			System.err.println("Unable to locate applicationName information holder for applicationName " + applicationName + "!");

			return false;
		}

		applicationPackage.addRegisteredSocketAddresses(nodeName, socketAddress);

		return true;
	}

	/**
	 * Queries for server-side TCP socket address. Called on setup of NodeGroups
	 * that have client-side TCP channels within Launcher. Server-side TCP channel
	 * informs manager about its address.
	 * 
	 * @param applicationName	Ditto.
	 * @param nodeName			Ditto.
	 * 
	 * @return The socket address associated with the requested TCP channel.
	 */
	public InetSocketAddress obtainSocketAddress(String applicationName, String nodeName) throws RemoteException {
		ApplicationPackage applicationPackage = applicationPackages.get(applicationName);

		if (applicationPackage == null) {
			System.err.println("Unable to locate applicationName information holder for applicationName " + applicationName + "!");

			return null;
		}

		while (applicationPackage.getRegisteredSocketAddress(nodeName) == null) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException exception) {
				// Don't worry, just try again...
			}
		}

		return applicationPackage.getRegisteredSocketAddress(nodeName);
	}

	/**
	 * Returns the aggregator specified by the application name and variable name.
	 * 
	 * @param applicationName	Ditto.
	 * @param variableName		Ditto.
	 * 
	 * @return The aggregator associated to the specified variable in the specified application. 
	 */
	public synchronized ApplicationAggregator<? extends Serializable, ? extends Serializable> obtainAggregator(String applicationName, String variableName) {
		ApplicationPackage applicationPackage = applicationPackages.get(applicationName);

		if (applicationPackage == null) {
			System.err.println("Unable to locate application package for application " + applicationName + "!");

			return null;
		}

		return applicationPackage.getApplicationSpecification().getAggregator(variableName);
	}


	/**
	 * Returns the controller specified by the application name and controller name.
	 * 
	 * @param applicationName	Ditto.
	 * @param controllerName 	Ditto.
	 * 
	 * @return The controller associated to the specified name in the specified application. 
	 */
	public synchronized ApplicationController obtainController(String applicationName, String controllerName) {
		ApplicationPackage applicationPackage = applicationPackages.get(applicationName);

		if (applicationPackage == null) {
			System.err.println("Unable to locate application information holder for application " + applicationName + "!");

			return null;
		}

		return applicationPackage.getApplicationSpecification().getController(controllerName);
	}

	/**
	 * Notifies master that a NodeGroup has finished execution. Called by Launchers.
	 * 
	 * @param resultSummary	Summary containing NodeGroup's runtime information.
	 * 
	 * @return True if scheduling works as expected; false otherwise.
	 */

	public synchronized boolean handleTermination(ResultSummary resultSummary) {
		String applicationName = resultSummary.getNodeGroupApplication();

		ApplicationPackage applicationPackage = applicationPackages.get(applicationName);

		if (applicationPackage == null) {
			System.err.println("Unable to locate application information holder for NodeGroup running " + resultSummary.getNodeGroupApplication() + " and with number " + resultSummary.getNodeGroupSerialNumber() + "!");

			return false;
		}

		Scheduler scheduler = applicationPackage.getApplicationScheduler();

		if (scheduler == null) {
			System.err.println("Unable to locate scheduler for NodeGroup running " + resultSummary.getNodeGroupApplication() + " with serial number " + resultSummary.getNodeGroupSerialNumber() + "!");

			return false;
		}

		if (!scheduler.handleTermination(resultSummary.getNodeGroupSerialNumber())) {
			System.err.println("Abnormal termination for NodeGroup running " + resultSummary.getNodeGroupApplication() + " with serial number " + resultSummary.getNodeGroupSerialNumber() + "!");

			finishApplication(resultSummary.getNodeGroupApplication());
			return false;
		}

		applicationPackage.addResultSummary(resultSummary);

		try {
			if (scheduler.finishedIteration()) {
				scheduler.terminateIteration();

				if (scheduler.finishedApplication()) {
					System.out.println("terminateApplication");
					scheduler.terminateApplication();

					finishApplication(resultSummary.getNodeGroupApplication());
				}
				else {
					System.out.println("prepareIteration");
					scheduler.prepareIteration();
					scheduler.schedule();
				}
			}
			else {
				scheduler.schedule();
			}

			return true;
		} catch (InsufficientLaunchersException exception) {
			System.err.println("Unable to proceed with scheduling of application " + resultSummary.getNodeGroupApplication() + "! Aborting application...");

			finishApplication(resultSummary.getNodeGroupApplication());
			return false;
		} catch (InexistentOutputException exception) {
			System.err.println("Unable to proceed with scheduling of application " + resultSummary.getNodeGroupApplication() + "! Aborting application...");

			finishApplication(resultSummary.getNodeGroupApplication());
			return false;
		} catch (InexistentInputException exception) {
			System.err.println("Unable to proceed with scheduling of application " + resultSummary.getNodeGroupApplication() + "! Aborting application...");

			finishApplication(resultSummary.getNodeGroupApplication());
			return false;
		}
	}

	/**
	 * Creates a holder containing the application name, specification, and scheduler, and makes it
	 * ready to start executing.
	 * 
	 * @param applicationName Name of the application.
	 * @param applicationSpecification Application specification.
	 * 
	 * @return The newly created holder.
	 * 
	 * @throws TemporalDependencyException If the application specification has a temporal dependency problem.
	 * @throws CyclicDependencyException If the application specification has a cyclic dependency problem.
	 * @throws InexistentInputException If one of the inputs for the first iteration are missing.
	 */
	private synchronized ApplicationPackage setupApplication(String applicationName, ApplicationSpecification applicationSpecification) throws TemporalDependencyException, CyclicDependencyException, InexistentInputException {
		ApplicationPackage applicationPackage = new ApplicationPackage();

		Scheduler applicationScheduler = new JobScheduler(applicationName);

		applicationPackage.setApplicationName(applicationName);
		applicationPackage.setApplicationSpecification(applicationSpecification);
		applicationPackage.setApplicationScheduler(applicationScheduler);

		applicationPackages.put(applicationName, applicationPackage);

		applicationPackage.markStart();

		applicationScheduler.prepareApplication();
		applicationScheduler.prepareIteration();

		return applicationPackage;
	}

	/**
	 * Deletes the holder containing the application name, specification, and scheduler, effectively
	 * finishing its execution.
	 * 
	 * @param applicationName Name of the application
	 * 
	 * @return True if the application was finished successfully; false otherwise.
	 */
	private synchronized boolean finishApplication(String applicationName) {
		ApplicationPackage applicationPackage = applicationPackages.get(applicationName);

		if (applicationPackage == null) {
			System.err.println("Unable to locate application information holder for application " + applicationName + "!");

			return false;
		}

		applicationPackages.remove(applicationName);

		applicationPackage.markFinish();

		processApplicationResult(applicationName, applicationPackage.getTotalRunningTime(), applicationPackage.getResultCollection());

		return true;
	}

	/**
	 * Returns the list of registered launchers.
	 * 
	 * @return The list of registered launchers.
	 */
	public Collection<Launcher> getRegisteredLaunchers() {
		return registeredLaunchers.values();
	}

	/**
	 * Returns the requested application package.
	 * 
	 * @param application The requested application.
	 * 
	 * @return The requested application package.
	 */
	public ApplicationPackage getApplicationPackage(String applicationName) {
		return applicationPackages.get(applicationName);
	}

	/**
	 * Generates summary of application execution.
	 * 
	 * @param applicationName		Application name
	 * @param runningTime			Application running time.
	 * @param applicationResultSummaries Result summaries obtained from application.
	 */
	private void processApplicationResult(String applicationName, long runningTime, Set<ResultSummary> applicationResultSummaries) {
		ResultGenerator resultGenerator = new ResultGenerator(baseDirectory, applicationName, runningTime, applicationResultSummaries);

		resultGenerator.start();
	}

	/**
	 * Override basic toString()
	 */
	public String toString() {
		return "Manager running on directory \"" + baseDirectory + "\"";
	}

	/**
	 * Manager startup method.
	 * 
	 * @param arguments A list containing:
	 *        1) The registry location;
	 *        2) The manager working directory.
	 */
	public static void main(String[] arguments) {
		System.out.println("Running " + JobManager.getInstance().toString());
	}
}
