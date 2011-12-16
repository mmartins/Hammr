package graphs.programs.pagerank;

import interfaces.ApplicationAggregator;

import java.rmi.RemoteException;
import java.util.Set;
import java.util.Map.Entry;

import mapreduce.communication.MRChannelElement;

import org.jgrapht.graph.DefaultDirectedGraph;

import utilities.Logging;

import communication.channel.ChannelElement;

import graphs.programs.GraphEdge;
import graphs.programs.GraphVertex;

public class DeciderFilePagerankWorker extends AbstractPagerankWorker {
	private static final long serialVersionUID = 1L;
	private ApplicationAggregator<Boolean, Boolean> aggregator;
	
	public DeciderFilePagerankWorker(int numberWorker, int numberVertexes, int numberWorkers) {
		super(numberWorker, numberVertexes, numberWorkers);
	}

	@Override
	protected DefaultDirectedGraph<GraphVertex, GraphEdge> createGraph() {
		return new DefaultDirectedGraph<GraphVertex,GraphEdge>(GraphEdge.class);
	}

	@Override
	protected void performActionNothingPresent() {
		Logging.Info("performActionNothingPresent");
		
		int count = 0;
		
		//send messages
		for(Entry<Integer, Double> entry : cache.mPagerank.entrySet())
		{
			GraphVertex vertex = vertexMap.get(String.valueOf(entry.getKey()));
			Set<GraphEdge> edgeSet = graph.outgoingEdgesOf(vertex);
			Set<GraphEdge> foreignEdgeSet = foreignEdges.get(vertex.getName());
			
			int fanout = edgeSet.size() + (foreignEdgeSet == null ? 0 : foreignEdgeSet.size());
			
			double pagerankContribution = fanout == 0 ? 0.0 : entry.getValue() / fanout;
			
			if (++count % 10000 == 0) {
				Logging.Info(String.format("processed %d vertex", count));
			}
			if(pagerankContribution > epsilon)
			{
				/**
				 * Update internal pagerank
				 */
				for (GraphEdge edge : edgeSet) {
					GraphVertex neighbor = graph.getEdgeTarget(edge);

					System.out.printf("worker %d send message to worker %d: %s %f\n", numberWorker,
							numberWorker, neighbor.getName(), pagerankContribution);
					
					write(new MRChannelElement<Integer, Double>(Integer.parseInt(neighbor.getName()),
							pagerankContribution), messageOutputFilename(numberWorker, numberWorker));
				}

				/**
				 * Send out messages
				 */
				if (foreignEdgeSet != null) {
					for (GraphEdge foreignEdge : foreignEdgeSet) {

						int ownerWorkerIndex = obtainOwnerWorkerIndex(foreignEdge.getTargetName());

						System.out.printf("worker %d send message to worker %d: %s %f\n", numberWorker,
								ownerWorkerIndex, foreignEdge.getTargetName(), pagerankContribution);

						write(new MRChannelElement<Integer, Double>(Integer.valueOf(foreignEdge.getTargetName()),
								pagerankContribution), messageOutputFilename(numberWorker, ownerWorkerIndex));
					}
				}
			}
		}
		
		boolean finish = true;
		
		for(Entry<Integer, Double> entry : cache.mPagerank.entrySet())
		{	
			if(Math.abs(entry.getValue() - mPrePagerank.get(entry.getKey())) >= epsilon)
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
		
	    try {
			nodeGroup.getCurrentLauncher().putCacheEntry(getCacheKey(), cache);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		terminate = true;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void performAction(ChannelElement channelElement) {
		assert(channelElement instanceof MRChannelElement<?,?>);
		
		MRChannelElement<Integer, Double> element = (MRChannelElement) channelElement;;
		
//		System.out.printf("worker %d get message: %d %f\n", numberWorker, element.getKey(), element.getValue());
		
		Double pagerank = cache.mPagerank.get(element.getKey());

		assert(pagerank != null);
		
		mNextPagerank.put(element.getKey(), pagerank + element.getValue());
		
	}

	@Override
	protected boolean loadCache()
	{
		boolean loadedFromCache = super.loadCache();
		if(!loadedFromCache)
		{
			cache.mPagerank.clear();
			cache.mPagerank.putAll(mPrePagerank);
		}
		return loadedFromCache;
	}
	
	@Override
	protected boolean performInitialization() {
		try {
			Logging.Info("perfortInitialization");
			
			loadCache();
			
			registerControlModule();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return true;
	}

	@Override
	void finishInitialization() {
		
	}

	@SuppressWarnings("unchecked")
	@Override
	void registerControlModule() {
		try {
			aggregator = (ApplicationAggregator<Boolean, Boolean>) nodeGroup.getManager().obtainAggregator("pagerank", "finish");
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		if(!performInitialization()) {
			return;
		}

		ChannelElement channelElement;

		
		Logging.Info("start receiving message");
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

		Logging.Info("done");	
		shutdown();		
	}

	@Override
	protected void performSpecialAction(MarkerChannelElement element) {
		//do nothing
	}
	
}
