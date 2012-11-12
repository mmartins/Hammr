/*
Copyright (c) 2011, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package utilities;

import graphs.communication.EdgeRecord;
import graphs.communication.VertexRecord;
import graphs.programs.GraphEdge;
import graphs.programs.GraphVertex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;

import utilities.filesystem.Directory;
import utilities.filesystem.FileHelper;
import utilities.filesystem.Filename;

import communication.writers.FileRecordWriter;

public abstract class GraphInputGenerator<V extends GraphVertex,E extends GraphEdge> {
	protected DefaultDirectedGraph<V,E> graph;

	protected Filename[] outputs;

	protected final int startIndex;
	
	public GraphInputGenerator(Directory directory, String[] outputs) {
		List<Filename> outputList = new ArrayList<Filename>();

		for (int i = 0; i < outputs.length; i++) {
			outputList.add(FileHelper.getFileInformation(directory.getPath(), outputs[i], directory.getProtocol()));
		}

		this.outputs = outputList.toArray(new Filename[outputList.size()]);
		startIndex = 0;
	}

	public GraphInputGenerator(Directory directory, int startIndex, String[] outputs) {
		List<Filename> outputList = new ArrayList<Filename>();

		for(int i = 0; i < outputs.length; i++) {
			outputList.add(FileHelper.getFileInformation(directory.getPath(), outputs[i], directory.getProtocol()));
		}

		this.outputs = outputList.toArray(new Filename[outputList.size()]);
		this.startIndex = startIndex;
	}
	
	protected abstract void obtainGraph();

	public void run() throws IOException {
		obtainGraph();

		// First, give name to all vertices and edges of the graph

		BreadthFirstIterator<V,E> iterator1 = new BreadthFirstIterator<V,E>(graph);

		int index = 0;

		while (iterator1.hasNext()) {
			V vertex = iterator1.next();

			vertex.setName(String.valueOf(startIndex + index++));
		}

		for (E edge: graph.edgeSet()) {
			V source = graph.getEdgeSource(edge);
			V target = graph.getEdgeTarget(edge);

			edge.setName(source.getName() + "," + target.getName());

			edge.setSourceName(source.getName());
			edge.setTargetName(target.getName());
		}

		// Now, write the vertices and edges to the input files

		FileRecordWriter[] writers = new FileRecordWriter[outputs.length];

		for (int i = 0; i < outputs.length; i++) {
			writers[i] =  new FileRecordWriter(outputs[i]);
		}

		int vertexPerWriter = (graph.vertexSet().size() / writers.length);
		int indexWrite = 0;

		BreadthFirstIterator<V,E> iterator2 = new BreadthFirstIterator<V,E>(graph);

		while (iterator2.hasNext()) {
			V vertex = iterator2.next();
			
			int writerIndex = Math.min(indexWrite / vertexPerWriter, outputs.length - 1);
			
			writers[writerIndex].write(new VertexRecord<V>(vertex));

			for (E edge: graph.outgoingEdgesOf(vertex)) {
				writers[writerIndex].write(new EdgeRecord<E>(edge));
			}

			indexWrite++;
		}

		for (int i = 0; i < writers.length; i++) {
			writers[i].close();
		}
	}
}