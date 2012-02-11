package utilities.pagerank;

import graphs.programs.GraphEdge;
import graphs.programs.GraphVertex;


import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.jgrapht.graph.DefaultDirectedGraph;

import utilities.GraphInputGenerator;
import utilities.filesystem.Directory;

/**
 * 
 * @author ljin
 *
 */

public class PageRankGraphInputGenerator extends GraphInputGenerator<GraphVertex, GraphEdge>{

	private final String spliter = "\t";
	private final String input;
	
	
	public PageRankGraphInputGenerator(Directory directory, String input, String[] outputs) {
		super(directory, outputs);
		this.input = input;
	}

	@Override
	protected void obtainGraph() {
		
		BufferedReader reader = null;
		try
		{
			reader = new BufferedReader(new FileReader(input));
			graph = new DefaultDirectedGraph<GraphVertex,GraphEdge>(GraphEdge.class);
			
			String buf;
			Map<Integer, GraphVertex> seenVids = new HashMap<Integer, GraphVertex>();
			
			while((buf = reader.readLine()) != null)
			{
				String[] tokens = buf.split(spliter);
				int[] vids = {Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1])};
				
				GraphVertex[] vs = {null, null};
				
				for(int i = 0; i < 2; ++i)
				{
					if((vs[i] = seenVids.get(vids[i])) == null)
					{
						vs[i] = new GraphVertex();
						vs[i].setName(String.valueOf(vids[i]));
						seenVids.put(vids[i], vs[i]);
						graph.addVertex(vs[i]);
					}
				}		
				graph.addEdge(vs[1], vs[0]);
			}	
			
			reader.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
	public static void main(String[] args) throws Exception
	{
		if(args.length <= 3) {
			System.err.println("usage: FileGraphInputGenerator baseDirectory input [<output> ... <output>]");

			System.exit(1);
		}
		
		String baseDir = args[0];
		String input = args[1];
		String[] outputs = Arrays.copyOfRange(args, 2, args.length);
		PageRankGraphInputGenerator generator = new PageRankGraphInputGenerator(new Directory(baseDir), input, outputs);
		generator.run();	
	}


	
}
