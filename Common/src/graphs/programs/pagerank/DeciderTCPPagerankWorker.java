package graphs.programs.pagerank;

import java.rmi.RemoteException;
import java.util.Map.Entry;

import communication.channel.ChannelElement;

import interfaces.ApplicationAggregator;

public class DeciderTCPPagerankWorker extends AbstractPagerankWorker{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static final double epsilon = 0.001;
	
	private ApplicationAggregator<Boolean, Boolean> aggregator;
	
	public DeciderTCPPagerankWorker(int numberWorker, int numberVertexes, int numberWorkers) {
		super(numberWorker, numberVertexes, numberWorkers);
	}

	@Override
	void finishInitialization() {
		//closeOutputs(getStructuralOutputChannels());	
	}

	@SuppressWarnings("unchecked")
	@Override
	void registerControlModule() {
		try {
			aggregator = (ApplicationAggregator<Boolean, Boolean>) nodeGroup.getManager().obtainAggregator("pagerank", "finish");
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void performActionNothingPresent() {
		boolean finish = true;
		
		for(Entry<Integer, Double> entry : mNextPagerank.entrySet())
		{
			double prePagerank = mPrePagerank.containsKey(entry.getKey()) ? 0.0 : mPrePagerank.get(entry.getKey());
			
			if(Math.abs(entry.getValue() - prePagerank) >= epsilon)
			{
				finish = false;
				break;
			}
		}
		
		try {			
			if(aggregator != null) aggregator.updateAggregate(finish);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		
		terminate = true;
	}	
	
	@Override
	public void run() {
		if(!performInitialization()) {
			return;
		}

		ChannelElement channelElement;

		while(true) {
			
			channelElement = readSomeone();

			if(channelElement == null) {
				performActionNothingPresent();
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

	@Override
	protected void performSpecialAction(MarkerChannelElement element) {
		// TODO Auto-generated method stub
		
	}
}
