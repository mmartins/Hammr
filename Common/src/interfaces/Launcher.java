package interfaces;

import java.util.List;

import java.rmi.Remote;
import java.rmi.RemoteException;

import execinfo.NodeGroup;

/**
 * Launcher remote interface. Called by manager.
 * 
 * @author Hammurabi Mendes (hmendes)
 */
public interface Launcher extends Remote {
	/**
	 * Returns launcher ID.
	 * 
	 * @return Launcher ID
	 */
	public String getId() throws RemoteException;

	/**
	 * Submits a NodeGroup for execution. Called by manager.
	 * 
	 * @param nodeGroup NodeGroup to be executed.
	 * 
	 * @return Always true.
	 */
	public boolean addNodeGroup(NodeGroup nodeGroup) throws RemoteException;
	
	/**
	 * Obtains the current running NodeGroups. Called by manager.
	 * 
	 * @return Current running NodeGroups.
	 */
	public List<NodeGroup> getNodeGroups() throws RemoteException;
}
