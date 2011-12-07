/*
Copyright (c) 2011, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package execinfo;

import interfaces.StateManager;
import execinfo.ProgressReport;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import utilities.RMIHelper;

public class Stage implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private long serialNumber;
	
	private ProgressReport progressReport;

	private Set<NodeGroup> nodeGroups;
	
	private StateManager stageManager;
	
	public Stage() {
		nodeGroups = new HashSet<NodeGroup>();		
		progressReport = new ProgressReport();
	}

	public Stage(NodeGroup nodeGroup) {
		nodeGroups = new HashSet<NodeGroup>();
		addNodeGroup(nodeGroup);
	}

	public Stage(Set<NodeGroup> nodeGroups) {
		nodeGroups = new HashSet<NodeGroup>();

		addNodeGroups(nodeGroups);
	}	

	public void setSerialNumber(long serialNumber) {
		this.serialNumber = serialNumber;
	}

	public long getSerialNumber() {
		return serialNumber;
	}
	
	public boolean addNodeGroup(NodeGroup nodeGroup) {
		if(nodeGroup.getStage() != null) {
			assert false;

			return false;
		}

		nodeGroup.setStage(this);

		nodeGroups.add(nodeGroup);

		return true;
	}

	public boolean addNodeGroups(Set<NodeGroup> nodeGroups) {
		for(NodeGroup nodeGroup: nodeGroups) {
			if(nodeGroup.getStage() != null) {
				assert false;

				return false;
			}
		}

		for(NodeGroup nodeGroup: nodeGroups) {
			nodeGroup.setStage(this);

			nodeGroups.add(nodeGroup);
		}	

		return true;
	}
	
	public Set<NodeGroup> getNodeGroups() {
		return nodeGroups;
	}
	
	public Iterator<NodeGroup> getNodeGroupsIterator() {
		return nodeGroups.iterator();
	}

	public int getSize() {
		return nodeGroups.size();
	}

	public void setStageManager(StateManager stageManager) {
		this.stageManager = stageManager;
	}
	
	public StateManager getStageManager() {
		return stageManager;
	}
	
	public ProgressReport getProgress() {
		return progressReport;
	}
	
	public void setProgressReport(ProgressReport progressReport) {
		this.progressReport.setProgress(progressReport.getProgress());
	}

	public ProgressReport updateProgress() {
		double progress = 0.0;
		Iterator<NodeGroup> iterator;
		
		iterator = getNodeGroupsIterator();
		
		while (iterator.hasNext()) {
			progress += iterator.next().getProgressReport().getProgress();
		}
		
		progress /= getSize();
		progressReport.setProgress(progress);
		
		return progressReport;
	}
	
	public String toString() {
		String result = "{\n";

		for (NodeGroup nodeGroup: nodeGroups) {
			result += "\t";
			result += nodeGroup;
			result += "\n";
		}

		result += "}";

		return result;
	}
}
