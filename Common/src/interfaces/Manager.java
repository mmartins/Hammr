package interfaces;

import java.rmi.Remote;
import java.rmi.RemoteException;

import java.net.InetSocketAddress;

import appspecs.ApplicationSpecification;

import execinfo.ResultSummary;

/**
 * Manager remote interface. Called by remote machines.
 * 
 * @author Hammurabi Mendes (hmendes)
 */
public interface Manager extends Remote {
	/**
	 * Notifies manager of new launcher being started. Called by Launchers.
	 * 
	 * @param launcher Started launcher.
	 * 
	 * @return True unless launcher is not reachable.
	 */
	public boolean registerLauncher(Launcher launcher) throws RemoteException;

	/**
	 * Submits a new application for execution. Called by clients.
	 * 
	 * @param applicationSpecification Specification of to-be-run application
	 * 
	 * @return True unless:
	 *         1) Application with the same name is already running;
	 *         2) Scheduler setup for application went wrong;
	 *         
	 */
	public boolean registerApplication(ApplicationSpecification applicationSpecification) throws RemoteException;

	/**
	 * Registers server-side TCP channel socket address with manager. Called
	 * within Laucher during setup of NodeGroups that have server-side TCP
	 * channels. The corresponding client-side TCP channels query master for
	 * this information.
	 * 
	 * @param applicationName
	 *            Ditto.
	 * @param nodeName
	 *            Ditto.
	 * @param socketAddress
	 *            Socket address of server-side TCP channel.
	 * 
	 * @return True unless the map for the specific pair applicationName/node
	 *         already exists.
	 */
	public boolean registerSocketAddress(String applicationName, String nodeName, InetSocketAddress socketAddress) throws RemoteException;

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
	public InetSocketAddress obtainSocketAddress(String applicationName, String nodeName) throws RemoteException;

	/**
	 * Notifies master that a NodeGroup has finished execution. Called by Launchers.
	 * 
	 * @param resultSummary	Summary containing NodeGroup's runtime information.
	 * 
	 * @return True if scheduling works as expected; false otherwise.
	 */
	public boolean handleTermination(ResultSummary resultSummary) throws RemoteException;
}
