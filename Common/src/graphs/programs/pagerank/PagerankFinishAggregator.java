package graphs.programs.pagerank;

import java.rmi.RemoteException;

import interfaces.ApplicationAggregator;

public class PagerankFinishAggregator implements ApplicationAggregator<Boolean, Boolean>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private boolean finish = true;
	
	public Boolean obtainAggregate() throws RemoteException {
		boolean ret = finish;
		finish = true;
		return ret;
	}

	public void updateAggregate(Boolean value) throws RemoteException {
		finish &= value; 
	}

}
