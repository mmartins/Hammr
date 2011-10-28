package manager;

import java.util.Set;
import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;

import java.net.InetSocketAddress;

import appspecs.ApplicationSpecification;

import scheduler.Scheduler;

import execinfo.ResultSummary;

/**
 * Packs information regarding an executing application in the manager. The information contained is
 * the application name, specification, scheduler, the current registered server-side TCP channels and
 * the result summaries for the finalized NodeGroups. 
 * 
 * @author Hammurabi Mendes (hmendes)
 * @author Marcelo Martins (martins)
 */
public class ApplicationPackage {
	private String applicationName;

	private ApplicationSpecification applicationSpecification;

	private Scheduler applicationScheduler;

	private Map<String, InetSocketAddress> registeredSocketAddresses;

	private Set<ResultSummary> resultCollection;
	
	private long globalTimerStart = -1L;
	private long globalTimerFinish = -1L;

	/**
	 * Class constructor.
	 */
	public ApplicationPackage() {
		this.registeredSocketAddresses = new HashMap<String, InetSocketAddress>();

		this.resultCollection = new HashSet<ResultSummary>();
	}

	/**
	 * Getter for the application name.
	 * 
	 * @return The application name.
	 */
	public String getApplicationName() {
		return applicationName;
	}

	/**
	 * Setter for the application name.
	 * 
	 * @param applicationName The new application name.
	 */
	public void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}

	/**
	 * Getter for the application specification.
	 * 
	 * @return The application specification.
	 */
	public ApplicationSpecification getApplicationSpecification() {
		return applicationSpecification;
	}

	/**
	 * Setter for the application specification.
	 * 
	 * @param applicationName The new application specification.
	 */
	public void setApplicationSpecification(ApplicationSpecification applicationSpecification) {
		this.applicationSpecification = applicationSpecification;
	}

	/**
	 * Getter for the application scheduler.
	 * 
	 * @return The application scheduler.
	 */
	public Scheduler getApplicationScheduler() {
		return applicationScheduler;
	}

	/**
	 * Setter for the application scheduler.
	 * 
	 * @param applicationName The new application scheduler.
	 */
	public void setApplicationScheduler(Scheduler applicationScheduler) {
		this.applicationScheduler = applicationScheduler;
	}

	/**
	 * Insert a socket address for a server-side TCP channel.
	 * 
	 * @param identifier Node that has a server-side TCP channel.
	 * @param registeredSocketAddress Socket address associated with the server-side TCP channel.
	 */
	public void addRegisteredSocketAddresses(String identifier, InetSocketAddress registeredSocketAddress) {
		registeredSocketAddresses.put(identifier, registeredSocketAddress);
	}

	/**
	 * Obtains the associated socket address for a particular Node's server-side TCP channel.
	 * 
	 * @param identifier Node name.
	 * 
	 * @return The socket address associated with the server-side TCP channel for the specified Node.
	 */
	public InetSocketAddress getRegisteredSocketAddress(String identifier) {
		return registeredSocketAddresses.get(identifier);
	}

	/**
	 * Inserts a received NodeGroup runtime information into holder.
	 * 
	 * @param resultSummary The received runtime information.
	 */
	public void addResultSummary(ResultSummary resultSummary) {
		resultCollection.add(resultSummary);
	}

	/**
	 * Obtains NodeGroup runtime information summary collection.
	 * 
	 * @return NodeGroup runtime information summary collection.
	 */
	public Set<ResultSummary> getResultCollection() {
		return resultCollection;
	}
	
	/**
	 * Registers application's (real) start time.
	 */
	public void markStart() {
		globalTimerStart = System.currentTimeMillis();
	}
	
	/**
	 * Registers application's (real) finish time.
	 */
	public void markFinish() {
		globalTimerFinish = System.currentTimeMillis();
	}
	
	/**
	 * Obtain application's total running (real) time.
	 * 
	 * @return Total running (real) time of application.
	 */
	public long getTotalRunningTime() {
		return globalTimerFinish - globalTimerStart;
	}
}
