/*
<<<<<<< HEAD
Copyright (c) 2010, Hammurabi Mendes
<<<<<<< HEAD

=======
>>>>>>> d398a6bce8b005e21fe66932959f36740382ee6d
=======
Copyright (c) 2011, Hammurabi Mendes
>>>>>>> hmendes/master
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package execinfo;

import interfaces.StateManager;
import interfaces.Manager;
import interfaces.Launcher;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import utilities.MutableInteger;
import appspecs.Node;

public class NodeGroup implements Serializable {
	private static final long serialVersionUID = 1L;

	/////////////////////////
	// RUNNING INFORMATION //
	/////////////////////////

	private Set<Node> nodes;

	private String applicationName;

	private long serialNumber;

	private Launcher currentLauncher;
	private Launcher previousLauncher;

	private Manager manager;
	private Stage stage;
	
	private StateManager groupManager;
	
	private MutableInteger mark;
	
	private ProgressReport progressReport;

	/////////////////////////
	// PARSING INFORMATION //
	/////////////////////////
	
	public NodeGroup(String applicationName, Node node) {
		nodes = new HashSet<Node>();

		setApplicationName(applicationName);
		addNode(node);
		ProgressReport progressReport = new ProgressReport();
	}

	public NodeGroup(String applicationName, Set<Node> nodes) {
		nodes = new HashSet<Node>();

		setApplicationName(applicationName);
		addNodes(nodes);
		ProgressReport progressReport = new ProgressReport();
	}

	public void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}

	public String getApplicationName() {
		return applicationName;
	}

	public void setSerialNumber(long serialNumber) {
		this.serialNumber = serialNumber;
	}

	public long getSerialNumber() {
		return serialNumber;
	}

	private boolean addNode(Node node) {
		if(node.getNodeGroup() != null) {
			assert false;

			return false;
		}

		node.setNodeGroup(this);

		nodes.add(node);

		return true;
	}

	private boolean addNodes(Collection<Node> nodes) {
		for(Node node: nodes) {
			if(node.getNodeGroup() != null) {
				assert false;

				return false;
			}
		}

		for(Node node: nodes) {
			node.setNodeGroup(this);

			nodes.add(node);
		}

		return true;
	}

	public Set<Node> getNodes() {
		return nodes;
	}

	public Iterator<Node> getNodesIterator() {
		return nodes.iterator();
	}

	public int getSize() {
		return nodes.size();
	}

	public Launcher getCurrentLauncher() {
		return currentLauncher;
	}

	public void setCurrentLauncher(Launcher currentLauncher) {
		this.currentLauncher = currentLauncher;
	}

	public Launcher getPreviousLauncher() {
		return previousLauncher;
	}

	public void setPreviousLauncher(Launcher previousLauncher) {
		this.previousLauncher = previousLauncher;
	}

	public Manager getManager() {
		return manager;
	}

	public void setManager(Manager manager) {
		this.manager = manager;
	}

	public MutableInteger getMark() {
		return mark;
	}

	public void setMark(MutableInteger mark) {
		if(!isMarked() || mark == null) {
			this.mark = mark;
		}
		else {
			this.mark.setValue(mark.getValue());
		}
	}

	public boolean isMarked() {
		return (mark != null);
	}

	public void setStage(Stage stage) {
		this.stage = stage;
	}

	public Stage getStage() {
		return stage;
	}
	
	public void setGroupManager(StateManager groupManager) {
		this.groupManager = groupManager;
	}
	
	public StateManager getGroupManager() {
		return groupManager;
	}
	
	public ProgressReport getProgressReport() {
		return progressReport;
	}

	public ProgressReport setProgressReport() {
		return progressReport;
	}
	
	public ProgressReport updateProgressReport() {
		double progress = 0.0;
		
		for (Node node: getNodes()) {
			progress += node.getProgressReport().getProgress();
		}
		progress /= getNodes().size();
		progressReport.setProgress(progress);
		
		return progressReport;
	}
	
	public void prepareSchedule(long serialNumber) {
		setSerialNumber(serialNumber);

		setStage(null);
		
		setMark(null);
	}

	public String toString() {
		String result = "[";

		boolean firstNode = true;

		for(Node node: getNodes()) {
			if(!firstNode) {
				result += ", ";
			}

			firstNode = false;

			result += node;
		}

		result += "]";

		return result;
	}
}
