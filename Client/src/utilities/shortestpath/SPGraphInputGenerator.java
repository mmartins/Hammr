/*
Copyright (c) 2011, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package utilities.shortestpath;

import java.io.IOException;

import java.util.Random;

import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import java.util.List;
import java.util.ArrayList;

import org.jgrapht.graph.DefaultDirectedGraph;


import utilities.GraphInputGenerator;
import utilities.filesystem.Directory;

import graphs.programs.GraphEdge;
import graphs.programs.GraphVertex;
import graphs.programs.shortestpath.SPGraphVertex;
import graphs.programs.shortestpath.SPGraphEdge;

public class SPGraphInputGenerator extends GraphInputGenerator<GraphVertex, GraphEdge> {
	
	private final int numberVertices;

	private final int numberEdgesPerVertex;

	public SPGraphInputGenerator(Directory directory, int startIndex, int numberVertices, int numberEdgesPerVertex,  String[] outputs) {
		super(directory, startIndex, outputs);
		
		this.numberVertices = numberVertices;

		this.numberEdgesPerVertex = numberEdgesPerVertex;
	}

	protected void obtainGraph() {
		obtainSpecifiedGraph();
	}

	protected void obtainSpecifiedGraph() {
		graph = new DefaultDirectedGraph<GraphVertex,GraphEdge>(GraphEdge.class);

		Random random = new Random();

		Map<Integer,SPGraphVertex> vertexMap = new HashMap<Integer,SPGraphVertex>();
		
		for(int i = 0; i < numberVertices; i++) {
			SPGraphVertex vertex = new SPGraphVertex();

			graph.addVertex(vertex);

			vertexMap.put(i, vertex);
			
			if(i % 10000 == 0)
				System.out.printf("processed %d vertex.\n", i - startIndex);
		}

		for(int i = 0; i < numberVertices; i++) {
			for(int j = 0; j < numberEdgesPerVertex; j++) {
				
				Set<Integer> visited = new HashSet<Integer>();
					
				int k = random.nextInt(numberVertices);
				
				while(visited.contains(k))
				{
					k = random.nextInt(numberVertices);
				}
				
				visited.add(k);
				
				graph.addEdge(vertexMap.get(i), vertexMap.get(k), new GraphEdge());
			}
			if(i % 1000 == 0)
				System.out.printf("processed %d vertex.\n", i);
		}		
	}

	protected void obtainSimpleGraph() {
		graph = new DefaultDirectedGraph<GraphVertex,GraphEdge>(GraphEdge.class);

		Map<Integer,SPGraphVertex> vertexMap = new HashMap<Integer,SPGraphVertex>();

		for(int i = 0; i < 5; i++) {
			SPGraphVertex vertex = new SPGraphVertex();

			graph.addVertex(vertex);

			vertexMap.put(i, vertex);
		}

		graph.addEdge(vertexMap.get(0), vertexMap.get(1), new SPGraphEdge(1));
		graph.addEdge(vertexMap.get(1), vertexMap.get(2), new SPGraphEdge(1));
		graph.addEdge(vertexMap.get(2), vertexMap.get(3), new SPGraphEdge(5));
		graph.addEdge(vertexMap.get(3), vertexMap.get(4), new SPGraphEdge(1));
		graph.addEdge(vertexMap.get(0), vertexMap.get(2), new SPGraphEdge(10));
		graph.addEdge(vertexMap.get(0), vertexMap.get(3), new SPGraphEdge(6));
	}

	public static void main(String[] arguments) {
		if(arguments.length <= 4) {
			System.err.println("usage: RandomGraphInputGenerator baseDirectory startIndex numberVertices numberEdgesPerVertex [<input> ... <input>]");

			System.exit(1);
		}

		String baseDirectory = arguments[0];

		int startIndex = Integer.valueOf(arguments[1]);
		
		int numberVertices = Integer.valueOf(arguments[2]);

		int numberEdgesPerVertex = Integer.valueOf(arguments[3]);

		List<String> outputsList = new ArrayList<String>();

		for(int i = 4; i < arguments.length; i++) {
			outputsList.add(arguments[i]);
		}

		String[] outputsArray = outputsList.toArray(new String[outputsList.size()]);

		SPGraphInputGenerator generator = new SPGraphInputGenerator(new Directory(baseDirectory), startIndex, numberVertices, numberEdgesPerVertex, outputsArray);

		try {
			generator.run();
		} catch (IOException exception) {
			System.err.println("Error generating input");

			exception.printStackTrace();
		}
	}
}
