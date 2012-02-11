package graphs.programs.pagerank;

import graphs.communication.EdgeRecord;
import graphs.communication.VertexRecord;
import graphs.programs.GraphEdge;
import graphs.programs.GraphVertex;
import graphs.programs.GraphWorker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import mapreduce.communication.MRRecord;

import org.jgrapht.graph.DefaultDirectedGraph;

import utilities.logging.Logging;

import communication.channel.Record;
import communication.channel.TCPOutputChannel;

public abstract class AbstractPagerankWorker extends GraphWorker<GraphVertex,GraphEdge> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final String messageInputFormat = "message-%d-%d";
	private static final String graphInputFormat = "graph.%d";
	private static final String pagerankInputFormat = "pagerank.%d";
	private static final String graphCacheKeyFormat = "pagerank-graph-%d";
	
	protected static final double epsilon = 0.0000001;
	
	Map<Integer, Double> mPrePagerank = new HashMap<Integer, Double>();
	Map<Integer, Double> mNextPagerank;
	
	private final String graphInputFilename;
	private final String pagerankInputFilename;
	
	protected PagerankCache cache;
	
	public static String messageInputFilename(int i, int j) {
		return String.format(messageInputFormat, i,j);
	}
	
	public static String messageOutputFilename(int i, int j)
	{
		return String.format(messageInputFormat, i,j) + ".out";
	}
	
	public static String graphInputFilename(int workerIndex)
	{
		return String.format(graphInputFormat, workerIndex);
	}
	
	public static String pagerankInputFilename(int workerIndex)
	{
		return String.format(pagerankInputFormat, workerIndex);
	}
	
	public static String pagerankOutputFilename(int workerIndex)
	{
		return String.format(pagerankInputFormat, workerIndex) + ".out";
	}
	
	protected String getCacheKey()
	{
		return String.format(graphCacheKeyFormat, numberWorker);
	}
	
	public AbstractPagerankWorker(int numberWorker, int numberVertexes, int numberWorkers) {
		super(numberWorker, numberVertexes, numberWorkers, 1, TimeUnit.SECONDS);
		graphInputFilename = graphInputFilename(numberWorker);
		pagerankInputFilename = pagerankInputFilename(numberWorker);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void performAction(Record record) {	
		if (record instanceof MRRecord<?, ?>) {
			MRRecord<Integer, Double> element = (MRRecord<Integer, Double>) record;

			System.out.printf("worker %d get message: %d %f.\n", numberWorker, element.getKey(), element.getValue());

			Double pagerank = null;

			if ((pagerank = mNextPagerank.get(element.getKey())) == null) {
				pagerank = 0.0;
			}

			mNextPagerank.put(element.getKey(), pagerank + element.getValue());
		}
		else if (record instanceof MarkerRecord)
		{
			performSpecialAction((MarkerRecord)record);
		}
			
	}

	protected abstract void performSpecialAction(MarkerRecord element);
	
	protected final void updateAndSendPagerank()
	{
		for (Entry<Integer, Double> entry : mPrePagerank.entrySet())
		{
			GraphVertex vertex = vertexMap.get(String.valueOf(entry.getKey()));
			Set<GraphEdge> edgeSet = graph.outgoingEdgesOf(vertex);
			Set<GraphEdge> foreignEdgeSet = foreignEdges.get(vertex.getName());
			
			int fanout = edgeSet.size() + (foreignEdgeSet == null ? 0 : foreignEdgeSet.size());
			
			double pagerankContribution = fanout == 0 ? 0.0 : entry.getValue() / fanout;
			
			
			if (pagerankContribution > epsilon)
			{
			/**
			 * Update internal pagerank
			 */
			for (GraphEdge edge : edgeSet)
			{
				GraphVertex neighbor = graph.getEdgeTarget(edge);
				int vertexIndex = Integer.parseInt(neighbor.getName());
				double pagerank = mNextPagerank.get(vertexIndex);
				mNextPagerank.put(vertexIndex, pagerank + pagerankContribution);
			}
			
			/**
			 * Send out messages
			 */
			if (foreignEdgeSet != null) {
				for (GraphEdge foreignEdge : foreignEdgeSet) {
				
					String ownerWorker = obtainOwnerWorker(foreignEdge.getTargetName());
					
					System.out.printf("worker %d send message to worker %s: %s %f\n", numberWorker, ownerWorker, foreignEdge.getTargetName(),
							pagerankContribution);

//					boolean ret = write(new MRRecord<Integer, Double>(Integer.valueOf(foreignEdge.getTargetName()),
//							pagerankContribution), ownerWorker);
					
					writeChannel(new MRRecord<Integer, Double>(Integer.valueOf(foreignEdge.getTargetName()),
							pagerankContribution), ownerWorker);
					
				}
			}
			}
		}
		
		Set<String> tcpOutputChannelNames = getOutputChannelNames(TCPOutputChannel.class);
		for (String channelName : tcpOutputChannelNames)
		{
			writeChannel(new MarkerRecord(null), channelName);
		}
		
	}
	
	@SuppressWarnings("unchecked")
	protected final MRRecord<Integer, Double> readPagerankRecord()
	{	
		return (MRRecord<Integer, Double>) readChannel(pagerankInputFilename);
	}
	
	/**
	 * mNextPagerank will be set to all zeros
	 */
	protected boolean loadCache() {
		boolean ret = false;
		try {
			Logging.log(String.format("%s: load cache",name));
			cache = (PagerankCache) nodeGroup.localLauncher.getCacheEntryLocal(getCacheKey());
			Logging.log(String.format("%s: finish loading cache",name));	
			
			if (cache != null) {
				edgeMap = cache.edgeMap;
				vertexMap = cache.vertexMap;
				foreignEdges = cache.foreignEdges;
				graph = cache.graph;
				mPrePagerank.putAll(cache.mPagerank);
				delInputChannel(graphInputFilename(numberWorker));
				delInputChannel(pagerankInputFilename(numberWorker));
				ret = true;
			} else {
				Logging.log(String.format("%s: loading graph...",name));
				loadGraph();
				Logging.log(String.format("%s: done",name));
				/**
				 * 
				 * read pagerank value from last iteration
				 */
				MRRecord<Integer, Double> element = null;

				while ((element = readPagerankRecord()) != null) {
					mPrePagerank.put(element.getKey(), element.getValue());
				}

				cache = new PagerankCache();
				cache.edgeMap = edgeMap;
				cache.vertexMap = vertexMap;
				cache.foreignEdges = foreignEdges;
				cache.graph = graph;
				cache.mPagerank = new HashMap<Integer, Double>();
				ret = false;
			}

			for (Integer i : mPrePagerank.keySet()) {
				cache.mPagerank.put(i, 0.0);
			}
			
			mNextPagerank = cache.mPagerank;
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return ret;	
	}
	
	@Override
	protected boolean performInitialization() {
		
		loadCache();
		
		registerControlModule();
		
		updateAndSendPagerank();
		
		finishInitialization();
		
		return true;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void loadGraph() {
		graph = createGraph();

			
		Record record;

		List<GraphVertex> vertexes = new ArrayList<GraphVertex>();
		List<GraphEdge> edges = new ArrayList<GraphEdge>();
		
		while (true) {
			record = readChannel(graphInputFilename);

			if (record == null) {
				break;
			}

			if (record instanceof VertexRecord<?>) {
				vertexes.add(((VertexRecord<GraphVertex>) record).getObject());
			}

			if (record instanceof EdgeRecord<?>) {
				edges.add(((EdgeRecord<GraphEdge>) record).getObject());
			}
		}

		// Add all vertices
		for (GraphVertex vertex : vertexes) {
			graph.addVertex(vertex);

			vertexMap.put(vertex.getName(), vertex);
		}

		// Add all edges
		for (GraphEdge edge : edges) {
			String sourceName = edge.getSourceName();
			String targetName = edge.getTargetName();

			GraphVertex sourceVertex = vertexMap.get(sourceName);
			GraphVertex targetVertex = vertexMap.get(targetName);

			// If this is a foreign neighbor, add the neighbor
			// to the foreign neighbor list. Otherwise, add the
			// edge to the local graph.

			if (targetVertex == null) {
				if (!foreignEdges.containsKey(sourceName)) {
					foreignEdges.put(sourceName, new HashSet<GraphEdge>());
				}

				foreignEdges.get(sourceName).add(edge);
			} else {
				graph.addEdge(sourceVertex, targetVertex, edge);
			}

			edgeMap.put(edge.getName(), edge);
		}
	}
	

	
	private void dumpPagerank()
	{	
		String output = pagerankOutputFilename(numberWorker);
		
		for (Entry<Integer, Double> entry : mNextPagerank.entrySet())
		{
			writeChannel(new MRRecord<Integer, Double>(entry.getKey(), entry.getValue()), output);
		}
	}
	
	@Override
	protected boolean performTermination() {
		Logging.log(String.format("%s: performTermination", name));
		dumpPagerank();
		
		return true;
	}

	@Override
	protected DefaultDirectedGraph<GraphVertex, GraphEdge> createGraph() {
		return new DefaultDirectedGraph<GraphVertex,GraphEdge>(GraphEdge.class);
	}

	abstract void registerControlModule();

	abstract void finishInitialization();

}
