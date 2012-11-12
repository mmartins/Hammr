package graphs.programs.pagerank;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import graphs.programs.GraphEdge;
import graphs.programs.GraphVertex;

import org.jgrapht.graph.DefaultDirectedGraph;

public class PagerankCache implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public DefaultDirectedGraph<GraphVertex, GraphEdge> graph;
	public Map<String, GraphVertex> vertexMap;
	public Map<String, GraphEdge> edgeMap;
	public Map<String, Set<GraphEdge>> foreignEdges;
	public Map<Integer, Double> mPagerank;
}
