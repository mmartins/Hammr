package execinfo;

import java.io.Serializable;

/**
 * Package for node timing measurements.
 * 
 * @author Hammurabi Mendes (hmendes)
 */
public class NodeMeasurements implements Serializable {
	private static final long serialVersionUID = 1L;

	private long realTime;
	private long cpuTime;
	private long userTime;

	/**
	 * Constructor method.
	 * 
	 * @param realTime	Node's execution real time.
	 * @param cpuTime	Node's execution CPU time.
	 * @param userTime	Node's execution user time.
	 */
	public NodeMeasurements(long realTime, long cpuTime, long userTime) {
		this.realTime = realTime;
		this.cpuTime = cpuTime;
		this.userTime = userTime;
	}

	/**
	 * Getter for the real time associated with the Node run.
	 * 
	 * @return Real time (epoch).
	 */
	public long getRealTime() {
		return realTime;
	}

	/**
	 * Getter for the CPU time associated with the Node run.
	 * 
	 * @return CPU time (epoch)
	 */
	public long getCpuTime() {
		return cpuTime;
	}

	/**
	 * Getter for user time associated with Node's run.
	 * 
	 * @return User time (epoch).
	 */
	public long getUserTime() {
		return userTime;
	}
}
