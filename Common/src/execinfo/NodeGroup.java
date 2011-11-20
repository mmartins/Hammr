package execinfo;

import java.io.Serializable;


import java.util.Collection;

import java.util.Set;
import java.util.HashSet;

import appspecs.Node;
import utilities.MutableInteger;

import execinfo.ProgressReport;

public class NodeGroup extends HashSet<Node> implements Serializable {
	private static final long serialVersionUID = 1L;

	private String applicationName;
	private long serialNumber;

	private MutableInteger mark;

	private Stage stage;
	
	private ProgressReport progressReport;
	
	public NodeGroup(String applicationName, Node node) {
		super();

		setApplicationName(applicationName);
		addNode(node);
		ProgressReport progressReport = new ProgressReport();
	}

	public NodeGroup(String applicationName, Set<Node> nodes) {
		super();

		setApplicationName(applicationName);
		addNodes(nodes);
	}

	public boolean add() {
		assert false;

		return false;
	}

	private boolean addNode(Node node) {
		if(node.getNodeGroup() != null) {
			assert false;

			return false;
		}

		node.setNodeGroup(this);

		super.add(node);

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

			super.add(node);
		}

		return true;
	}

	public Set<Node> getNodes() {
		return this;
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
