package execinfo;

import java.io.Serializable;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import execinfo.NodeGroup;
import execinfo.ProgressReport;

public class Stage extends HashSet<NodeGroup> implements Serializable {
	private static final long serialVersionUID = 1L;
	private ProgressReport progressReport;

	public Stage() {
		super();
		progressReport = new ProgressReport();
	}

	public Stage(NodeGroup nodeGroup) {
		this();

		addNodeGroup(nodeGroup);
	}

	public Stage(Set<NodeGroup> nodeGroups) {
		this();

		addNodeGroups(nodeGroups);
	}	

	public boolean add() {
		assert false;

		return false;
	}

	public boolean addNodeGroup(NodeGroup nodeGroup) {
		if(nodeGroup.getStage() != null) {
			assert false;

			return false;
		}

		nodeGroup.setStage(this);

		super.add(nodeGroup);

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

			super.add(nodeGroup);
		}	

		return true;
	}
	
	public ProgressReport getProgress() {
		return this.progressReport;
	}
	
	public void setProgressReport(ProgressReport progressReport) {
		this.progressReport.setProgress(progressReport.getProgress());
	}

	public ProgressReport updateProgress() {
		double progress = 0.0;
		Iterator<NodeGroup> iterator;
		
		iterator = super.iterator();
		
		while (iterator.hasNext()) {
			progress += iterator.next().getProgressReport().getProgress();
		}
		
		progress /= super.size();
		progressReport.setProgress(progress);
		
		return progressReport;
	}
	
	public String toString() {
		String result = "{\n";

		for (NodeGroup nodeGroup: this) {
			result += "\t";
			result += nodeGroup;
			result += "\n";
		}

		result += "}";

		return result;
	}
}
