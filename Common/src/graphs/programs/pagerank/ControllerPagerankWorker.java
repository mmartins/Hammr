package graphs.programs.pagerank;

import interfaces.ApplicationController;
import interfaces.ExportableActivatable;

import java.rmi.RemoteException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import utilities.Logging;

import communication.channel.ChannelElement;

public class ControllerPagerankWorker extends AbstractPagerankWorker implements ExportableActivatable{

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
	protected void performSpecialAction(MarkerChannelElement element) {
		counter--;
	}

	@Override
	public boolean isActive() throws RemoteException {
		return active;
	}

	@Override
	public void setActive(boolean active) throws RemoteException {
		this.active = true;;
		this.terminate = !active;
		//Logging.Info("activated");
		lock.lock();
		try {
			cond.signal();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void run() {
		if(!performInitialization()) {
			return;
		}

		ChannelElement channelElement;

		while(true) {
			
			channelElement = tryReadSomeone(timeout, timeUnit);

			if(channelElement == null) {
				performActionNothingPresent();
				if(!terminate)
				{
					Logging.Info("Next iteration");
					mPrePagerank.clear();
					mPrePagerank.putAll(cache.mPagerank);
					for(Integer i : mPrePagerank.keySet())
					{
						cache.mPagerank.put(i, 0.0);
					}
					
					updateAndSendPagerank();	
					finishInitialization();
				}
			}
			else {
				performAction(channelElement);
			}

			if(terminate) {
				break;
			}
		}

		performTermination();

		shutdown();		
	}
	
}
