package graphs.programs.pagerank;

import interfaces.ApplicationController;
import interfaces.ExportableActivatable;

import java.rmi.RemoteException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import utilities.logging.Logging;

import communication.channel.Record;

public class ControllerPagerankWorker extends AbstractPagerankWorker implements ExportableActivatable{

	private static final long serialVersionUID = 1L;
	
	private int counter;
	private ApplicationController controller;
	boolean active = true;
	final Lock lock = new ReentrantLock();
	final Condition cond = lock.newCondition();
	
	public ControllerPagerankWorker(int numberWorker, int numberVertexes, int numberWorkers) {
		super(numberWorker, numberVertexes, numberWorkers);
	}

	@Override
	void finishInitialization() {
		counter = numberWorkers - 1;
	}

	@Override
	void registerControlModule() {
		try {
			this.controller = nodeGroup.getManager().obtainController("pagerank", "finish");
		} catch (RemoteException exception) {
			exception.printStackTrace();
		}
		
		try {
			controller.notifyStart(this);
		} catch (RemoteException exception) {
			System.err.println("Error updating the controller: " + exception);
		}	
	}

	@Override
	protected void performActionNothingPresent() {
		assert(counter == 0);
		
		System.out.println(mNextPagerank);
		
		active = false;
		
		try {
			controller.notifyFinish(this);
		} catch (RemoteException exception) {
			System.err.println("Error updating the controller: " + exception);
		}
		
		try {
			lock.lock();
			while (!active) {
				cond.await();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			lock.unlock();
		}
	}

	@Override
	protected void performSpecialAction(MarkerRecord element) {
		counter--;
	}

	public boolean isActive() throws RemoteException {
		return active;
	}

	public void setActive(boolean active) throws RemoteException {
		this.active = true;
		this.terminate = !active;
		// Logging.log("activated");
		lock.lock();
		try {
			cond.signal();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void run() {
		if (!performInitialization()) {
			return;
		}

		Record record;

		while (true) {
			
			record = tryReadArbitraryChannel(timeout, timeUnit);

			if (record == null) {
				performActionNothingPresent();
				if (!terminate)
				{
					Logging.log("Next iteration");
					mPrePagerank.clear();
					mPrePagerank.putAll(cache.mPagerank);
					for (Integer i : mPrePagerank.keySet())
					{
						cache.mPagerank.put(i, 0.0);
					}
					
					updateAndSendPagerank();	
					finishInitialization();
				}
			}
			else {
				performAction(record);
			}

			if (terminate) {
				break;
			}
		}

		performTermination();

		shutdown();		
	}
	
}
