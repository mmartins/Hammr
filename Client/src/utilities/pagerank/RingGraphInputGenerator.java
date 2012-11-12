package utilities.pagerank;

import graphs.programs.GraphEdge;
import graphs.programs.GraphVertex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jgrapht.graph.DefaultDirectedGraph;

import utilities.GraphInputGenerator;
import utilities.filesystem.Directory;

public class RingGraphInputGenerator extends GraphInputGenerator<GraphVertex, GraphEdge> {
	private final int numberVertices;
	
	public RingGraphInputGenerator(Directory directory, int numberVertices, String[] outputs) {
		super(directory, outputs);
		this.numberVertices = numberVertices;
		
	}
	
	@Override
	protected void obtainGraph() {
		graph = new DefaultDirectedGraph<GraphVertex,GraphEdge>(GraphEdge.class);
		
		Map<Integer,GraphVertex> vertexMap = new HashMap<Integer,GraphVertex>();
		
		for(int i = 0; i < numberVertices; i++) {
			GraphVertex vertex = new GraphVertex();

			graph.addVertex(vertex);

			vertexMap.put(i, vertex);
		}
		
		for(int i = 0; i < numberVertices; ++i)
		{
			graph.addEdge(vertexMap.get(i), vertexMap.get((i+1)%numberVertices), new GraphEdge());
			graph.addEdge(vertexMap.get(i), vertexMap.get(i), new GraphEdge());
			graph.addEdge(vertexMap.get(i), vertexMap.get((i-1+numberVertices)%numberVertices), new GraphEdge());
		}
		
	}
	
	public static void main(String[] arguments) {
		if(arguments.length < 2) {
			System.err.println("usage: RingGraphInputGenerator baseDirectory numberVertices [<input> ... <input>]");

			System.exit(1);
		}

		String baseDirectory = arguments[0];

		int numberVertices = Integer.valueOf(arguments[1]);

		List<String> outputsList = new ArrayList<String>();

		for(int i = 2; i < arguments.length; i++) {
			outputsList.add(arguments[i]);
		}

		String[] outputsArray = outputsList.toArray(new String[outputsList.size()]);

		RingGraphInputGenerator generator = new RingGraphInputGenerator(new Directory(baseDirectory), numberVertices, outputsArray);

		try {
			generator.run();
		} catch (IOException exception) {
			System.err.println("Error generating input");
			exception.printStackTrace();
		}
	}


}
