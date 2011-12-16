package graphs.programs.pagerank;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import utilities.Logging;

import interfaces.ApplicationController;
import interfaces.Exportable;
import interfaces.ExportableActivatable;

public class PagerankController implements ApplicationController{

	private static final long serialVersionUID = 1L;

	private final int numberWorkers;

	private int finishedWorkerCount;
	
	private List<Exportable> workers;
	
	private int counter = 50;
	
	public PagerankController(int numberWorkers) {
		this.numberWorkers = numberWorkers;

		this.workers = new ArrayList<Exportable>();
		
		finishedWorkerCount = 0;
	}
	
	@Override
	public synchronized void notifyFinish(Exportable node) throws RemoteException {
		
		if(++finishedWorkerCount == numberWorkers) {
			finishedWorkerCount = 0;
			if(counter == 0)
			{
				terminateWorkers();
			}
			else
			{
				counter--;
				activeWorkers();
			}
		}	
		
	}

	private void activeWorkers() {
		for(Exportable worker: workers) {
			try {
				if(worker instanceof ExportableActivatable) {
					((ExportableActivatable) worker).setActive(true);
				}
			} catch (RemoteException exception) {
				System.err.println("Unable to contact activatable node");
			}
		}
	}

	private void terminateWorkers() {
		for(Exportable worker: workers) {
			try {
				if(worker instanceof ExportableActivatable) {
					((ExportableActivatable) worker).setActive(false);
				}
			} catch (RemoteException exception) {
				System.err.println("Unable to contact activatable node");
			}
		}
	}

	@Override
	public synchronized void notifyStart(Exportable node) throws RemoteException {
		workers.add(node);
	}

}
