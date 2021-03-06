/*
Copyright (c) 2011, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package mapreduce.appspecs;

import utilities.filesystem.Directory;
import utilities.filesystem.Filename;
import appspecs.ApplicationSpecification;
import appspecs.Node;

import enums.CommunicationMode;

import exceptions.InexistentInputException;
import exceptions.OverlapingFilesException;

public class MapReduceSpecification extends ApplicationSpecification {
	private static final long serialVersionUID = 1L;

	private Node[] splitStage;

	private Node[] mapStage;
	private Node[] reduceStage;

	private Node[] mergeStage;

	public MapReduceSpecification(String name, Directory baseDirectory) {
		super(name, baseDirectory);
	}

	public void insertMappers(Filename input, Node splitter, Node[] mappers) throws InexistentInputException {
		stageSplitter(splitter);

		addInput(splitter, input);

		stageMappers(mappers);

		insertEdges(splitStage, mapStage, CommunicationMode.FILE);
	}

	public void insertMappers(Filename[] inputs, Node[] mappers) throws InexistentInputException {
		stageMappers(mappers);

		for (int i = 0; i < inputs.length; i++) {
			addInput(mappers[i], inputs[i]);
		}
	}

	public void insertReducers(Filename output, Node merger, Node[] reducers) throws OverlapingFilesException {
		stageReducers(reducers);

		stageMerger(merger);

		addOutput(mergeStage[0], output);

		insertEdges(reduceStage, mergeStage, CommunicationMode.FILE);
	}

	public void insertReducers(Filename[] outputs, Node[] reducers) throws OverlapingFilesException {
		stageReducers(reducers);

		for(int i = 0; i < outputs.length; i++) {
			addOutput(reducers[i], outputs[i]);
		}
	}

	public void setupCommunication(boolean useTCP) throws OverlapingFilesException {
		if(useTCP) {
			insertEdges(mapStage, reduceStage, CommunicationMode.TCP);
		}
		else {
			insertEdges(mapStage, reduceStage, CommunicationMode.FILE);
		}

		finalize();
	}

	private void stageSplitter(Node splitter) {
		nameGenerationString = "splitter-";
		nameGenerationCounter = 0L;

		splitStage = new Node[1];

		splitStage[0] = splitter;

		insertNodes(splitStage);
	}

	private void stageMerger(Node merger) {
		nameGenerationString = "merger-";
		nameGenerationCounter = 0L;

		mergeStage = new Node[1];

		mergeStage[0] = merger;

		insertNodes(mergeStage);
	}

	private void stageMappers(Node[] mappers) {
		mapStage = mappers;

		setupMapperNaming();
		insertNodes(mapStage);
	}

	private void stageReducers(Node[] reducers) {
		reduceStage = reducers;

		setupReducerNaming();
		insertNodes(reduceStage);
	}

	private void setupMapperNaming() {
		nameGenerationString = "mapper-";
		nameGenerationCounter = 0L;
	}

	private void setupReducerNaming() {
		nameGenerationString = "reducer-";
		nameGenerationCounter = 0L;
	}
}
