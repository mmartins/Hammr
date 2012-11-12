/*
Copyright (c) 2010, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package interfaces;

import java.net.InetSocketAddress;
import java.rmi.Remote;
import java.rmi.RemoteException;

import java.io.Serializable;

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
	 * @param applicationSpecification
	 *            Specification of to-be-run application
	 * 
	 * @return True unless:
     *		1) Application with the same name is already running;
     *		2) Scheduler setup for application went wrong;
	 * 
	 */
	public boolean registerApplication(ApplicationSpecification applicationSpecification) throws RemoteException;

	/**
	 * Registers server-side TCP channel socket address with manager. Called
	 * within Laucher during setup of NodeGroups that have server-side TCP
	 * channels. The corresponding client-side TCP channels query master for
	 * this information.
	 * 
	 * @param applicationName Ditto.
	 * @param nodeName Ditto.
	 * @param socketAddress Socket address of server-side TCP channel.
	 * 
	 * @return True unless the map for the specific pair applicationName/node
     *          already exists.
	 */
	public boolean registerSocketAddress(String applicationName, String nodeName, InetSocketAddress socketAddress) throws RemoteException;

	/**
	 * Queries for server-side TCP socket address. Called on setup of NodeGroups
	 * that have client-side TCP channels within Launcher. Server-side TCP
	 * channel informs manager about its address.
	 * 
	 * @param applicationName Ditto.
	 * @param nodeName Ditto.
	 * 
	 * @return The socket address associated with the requested TCP channel.
	 */
	public InetSocketAddress obtainSocketAddress(String applicationName, String nodeName) throws RemoteException;

	/**
	 * Returns the aggregator specified by the application name and variable
	 * name.
	 * 
	 * @param application	The application name.
	 * @param variable		The variable name;
	 * 
	 * @return The aggregator associated to the specified variable in the
	 *         specified application.
	 */
	public ApplicationAggregator<? extends Serializable, ? extends Serializable> obtainAggregator(String applicationName, String variable) throws RemoteException;

	/**
	 * Returns the controller specified by the application name and controller name.
	 * 
	 * @param application The application name.
	 * @param name The controller name;
	 * 
	 * @return The controller associated to the specified name in the specified application. 
	 */
	public ApplicationController obtainController(String application, String name) throws RemoteException;


	/**
	 * Notifies the master that a NodeGroup finished execution. This is called
	 * by the Launchers.
	 * 
	 * @param resultSummary Summary containing NodeGroup's runtime information.
	 * 
	 * @return True if scheduling works as expected; false otherwise.
	 */
	public boolean handleTermination(ResultSummary resultSummary) throws RemoteException;
}
