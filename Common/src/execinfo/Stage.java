package execinfo;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import execinfo.NodeGroup;

public class Stage extends HashSet<NodeGroup> implements Serializable {
	private static final long serialVersionUID = 1L;

	public Stage() {
		super();
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

	public String toString() {
		String result = "{\n";

		for(NodeGroup nodeGroup: this) {
			result += "\t";
			result += nodeGroup;
			result += "\n";
		}

		result += "}";

		return result;
	}
}
