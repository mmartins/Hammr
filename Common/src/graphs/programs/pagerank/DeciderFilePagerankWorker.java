package graphs.programs.pagerank;

import graphs.programs.GraphEdge;
import graphs.programs.GraphVertex;
import interfaces.ApplicationAggregator;

import java.rmi.RemoteException;
import java.util.Map.Entry;
import java.util.Set;

import mapreduce.communication.MRRecord;

import org.jgrapht.graph.DefaultDirectedGraph;

import utilities.logging.Logging;

import communication.channel.Record;

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
		Logging.log(String.format("%s: performActionNothingPresent",name));
		
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
				Logging.log(String.format("%s: processed %d vertex", name, count));
			}
			if(pagerankContribution > epsilon)
			{
				/**
				 * Update internal pagerank
				 */
				for (GraphEdge edge : edgeSet) {
					GraphVertex neighbor = graph.getEdgeTarget(edge);

//					System.out.printf("worker %d send message to worker %d: %s %f\n", numberWorker,
//							numberWorker, neighbor.getName(), pagerankContribution);
					
					writeChannel(new MRRecord<Integer, Double>(Integer.parseInt(neighbor.getName()),
							pagerankContribution), messageOutputFilename(numberWorker, numberWorker));
				}

				/**
				 * Send out messages
				 */
				if (foreignEdgeSet != null) {
					for (GraphEdge foreignEdge : foreignEdgeSet) {

						int ownerWorkerIndex = obtainOwnerWorkerIndex(foreignEdge.getTargetName());

//						System.out.printf("worker %d send message to worker %d: %s %f\n", numberWorker,
//								ownerWorkerIndex, foreignEdge.getTargetName(), pagerankContribution);

						writeChannel(new MRRecord<Integer, Double>(Integer.valueOf(foreignEdge.getTargetName()),
								pagerankContribution), messageOutputFilename(numberWorker, ownerWorkerIndex));
					}
				}
			}
		}
		
		boolean finish = true;
		
		for (Entry<Integer, Double> entry : cache.mPagerank.entrySet())
		{	
			if (Math.abs(entry.getValue() - mPrePagerank.get(entry.getKey())) >= epsilon)
			{
				finish = false;
				break;
			}
		}
		
		try {			
			if (aggregator != null) aggregator.updateAggregate(finish);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		
		nodeGroup.localLauncher.putCacheEntryLocal(getCacheKey(), cache);
		
		terminate = true;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void performAction(Record record) {
		assert(record instanceof MRRecord<?,?>);
		
		MRRecord<Integer, Double> element = (MRRecord<Integer, Double>) record;;
		
//		System.out.printf("worker %d get message: %d %f\n", numberWorker, element.getKey(), element.getValue());
		
		Double pagerank = cache.mPagerank.get(element.getKey());

		assert(pagerank != null);
		
		mNextPagerank.put(element.getKey(), pagerank + element.getValue());
		
	}

	@Override
	protected boolean loadCache()
	{
		boolean loadedFromCache = super.loadCache();
		if (!loadedFromCache)
		{
			cache.mPagerank.clear();
			cache.mPagerank.putAll(mPrePagerank);
		}
		return loadedFromCache;
	}
	
	@Override
	protected boolean performInitialization() {
		try {
			Logging.log(String.format("%s: perfortInitialization", name));
			
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

		Record record;

		
		Logging.log(String.format("%s: start receiving message", name));
		while (true) {
			
			record = readArbitraryChannel();

			if(record == null) {
				performActionNothingPresent();
			}
			else {
				performAction(record);
			}

			if(terminate) {
				break;
			}
		}

		performTermination();

		Logging.log(String.format("%s: done",name));	
		shutdown();		
	}

	@Override
	protected void performSpecialAction(MarkerRecord element) {
		//do nothing
	}
	
}
