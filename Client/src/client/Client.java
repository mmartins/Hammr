/*
Copyright (c) 2011, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package client;

import interfaces.Manager;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import mapreduce.appspecs.MapReduceSpecification;
import mapreduce.programs.counting.CountingMapper;
import mapreduce.programs.counting.CountingMerger;
import mapreduce.programs.counting.CountingReducer;
import nodes.ReaderArbitraryWriterArbitrary;
import nodes.TrivialNode;
import utilities.RMIHelper;
import utilities.filesystem.Directory;
import utilities.filesystem.FileHelper;
import utilities.filesystem.Filename;
import appspecs.ApplicationSpecification;
import appspecs.Node;
import enums.CommunicationMode;
import exceptions.InexistentInputException;
import exceptions.OverlapingFilesException;

public class Client {
	private String registryLocation;

	private Directory baseDirectory;

	public Client(String registryLocation, Directory baseDirectory) {
		this.registryLocation = registryLocation;

		this.baseDirectory = baseDirectory;
	}

	public void performTest1() {
		Manager manager = (Manager) RMIHelper.locateRemoteObject(registryLocation, "Manager");

		ApplicationSpecification applicationSpecification = new ApplicationSpecification("test1", baseDirectory);

		Filename inputFilename;
		Filename outputFilename;

		Node[] nodesStage1 = new Node[1];

		for (int i = 0; i < nodesStage1.length; i++) {
			nodesStage1[i] = new ReaderArbitraryWriterArbitrary();
		}

		applicationSpecification.insertNodes(nodesStage1);

		for (int i = 0; i < nodesStage1.length; i++) {
			inputFilename = FileHelper.getFileInformation(baseDirectory.getPath(), "input-stage1-" + i + ".dat", baseDirectory.getProtocol());

			applicationSpecification.addInput(nodesStage1[i], inputFilename);
		}

		for (int i = 0; i < nodesStage1.length; i++) {
			outputFilename = FileHelper.getFileInformation(baseDirectory.getPath(), "output-stage1-" + i + ".dat", baseDirectory.getProtocol());

			try {
				applicationSpecification.addOutput(nodesStage1[i], outputFilename);
			} catch (OverlapingFilesException exception) {
				System.err.println(exception);

				System.exit(1);
			}
		}

		try {
			applicationSpecification.finalize();
		} catch (OverlapingFilesException exception) {
			System.err.println(exception);

			System.exit(1);
		}

		try {
			manager.registerApplication(applicationSpecification);
		} catch (RemoteException exception) {
			System.err.println("Unable to contact manager");
			exception.printStackTrace();

			System.exit(1);
		}
	}

	public void performTest2(CommunicationMode communicationType) {
		Manager manager = (Manager) RMIHelper.locateRemoteObject(registryLocation, "Manager");

		ApplicationSpecification applicationSpecification = new ApplicationSpecification("test2", baseDirectory);

		Filename inputFilename;
		Filename outputFilename;

		Node[] nodesStage1 = new Node[1];

		for (int i = 0; i < nodesStage1.length; i++) {
			nodesStage1[i] = new ReaderArbitraryWriterArbitrary();
		}

		applicationSpecification.insertNodes(nodesStage1);

		for (int i = 0; i < nodesStage1.length; i++) {
			inputFilename = FileHelper.getFileInformation(baseDirectory.getPath(), "input-stage1-" + i + ".dat", baseDirectory.getProtocol());

			applicationSpecification.addInput(nodesStage1[i], inputFilename);
		}

		Node[] nodesStage2 = new Node[1];

		for (int i = 0; i < nodesStage2.length; i++) {
			nodesStage2[i] = new ReaderArbitraryWriterArbitrary();
		}

		applicationSpecification.insertNodes(nodesStage2);

		for (int i = 0; i < nodesStage2.length; i++) {
			outputFilename = FileHelper.getFileInformation(baseDirectory.getPath(), "output-stage2-" + i + ".dat", baseDirectory.getProtocol());

			try {
				applicationSpecification.addOutput(nodesStage2[i], outputFilename);
			} catch (OverlapingFilesException exception) {
				System.err.println(exception);

				System.exit(1);
			}
		}

		applicationSpecification.insertEdges(nodesStage1, nodesStage2, communicationType, -1);

		try {
			applicationSpecification.finalize();
		} catch (OverlapingFilesException exception) {
			System.err.println(exception);

			System.exit(1);
		}

		try {
			manager.registerApplication(applicationSpecification);
		} catch (RemoteException exception) {
			System.err.println("Unable to contact manager");
			exception.printStackTrace();

			System.exit(1);
		}
	}

	public void performTest3(int numberNodesEdges) {
		Manager manager = (Manager) RMIHelper.locateRemoteObject(registryLocation, "Manager");

		ApplicationSpecification applicationSpecification = new ApplicationSpecification("test3", baseDirectory);

		Node[] nodes = new Node[numberNodesEdges];

		for (int i = 0; i < nodes.length; i++) {
			nodes[i] = new TrivialNode();
		}

		applicationSpecification.insertNodes(nodes);

		Filename inputFilename = FileHelper.getFileInformation(baseDirectory.getPath(), "fake-input.dat", baseDirectory.getProtocol());

		applicationSpecification.addInput(nodes[0], inputFilename);

		Random random = new Random();

		int dice;

		Node[] source = new Node[1];
		Node[] target = new Node[1];

		int numberEdges = 0;

		for (int i = 0; i < nodes.length; i++) {
			for (int j = i + 1; j < nodes.length; j++) {
				dice = random.nextInt() % 1000;

				if (dice <= 0) {
					source[0] = nodes[i];
					target[0] = nodes[j];

					applicationSpecification.insertEdges(source, target, CommunicationMode.TCP);
					numberEdges++;
				}
			}
		}

		System.out.println("Edges created: " + numberEdges);

		try {
			applicationSpecification.finalize();
		} catch (OverlapingFilesException exception) {
			System.err.println(exception);

			System.exit(1);
		}

		try {
			manager.registerApplication(applicationSpecification);
		} catch (RemoteException exception) {
			System.err.println("Unable to contact manager");
			exception.printStackTrace();

			System.exit(1);
		}
	}

	public void performMapReduce(String[] inputs, boolean useTCP, boolean initialSplit, boolean finalMerge) {
		int numberMappers;
		int numberReducers;

		numberMappers = numberReducers = inputs.length;

		Manager manager = (Manager) RMIHelper.locateRemoteObject(registryLocation, "Manager");

		// Create the MapReduce specification

		MapReduceSpecification mapReduceSpecification = new MapReduceSpecification("mapreduce", baseDirectory);

		// Insert the mappers

		Node[] nodesStage1 = new Node[numberMappers];

		for (int i = 0; i < nodesStage1.length; i++) {
			nodesStage1[i] = new CountingMapper<String>(numberReducers);
		}

		try {
			if (initialSplit) {
				mapReduceSpecification.insertMappers(FileHelper.getFileInformation(baseDirectory.getPath(), "input-splitter.dat", baseDirectory.getProtocol()), new ReaderArbitraryWriterArbitrary(), nodesStage1);
			}
			else {
				// Create the input filenames

				Filename[] inputFilenames = new Filename[numberMappers];

				for (int i = 0; i < inputs.length; i++) {
					inputFilenames[i] = FileHelper.getFileInformation(baseDirectory.getPath(), inputs[i], baseDirectory.getProtocol());
				}

				mapReduceSpecification.insertMappers(inputFilenames, nodesStage1);
			}
		} catch (InexistentInputException exception) {
			System.err.println(exception);

			System.exit(1);
		}

		// Insert the reducers

		Node[] nodesStage2 = new Node[numberReducers];

		for (int i = 0; i < nodesStage2.length; i++) {
			nodesStage2[i] = new CountingReducer<String>();
		}

		try {
			if(finalMerge) {
				mapReduceSpecification.insertReducers(FileHelper.getFileInformation(baseDirectory.getPath(), "output-merger.dat", baseDirectory.getProtocol()), new CountingMerger<String>(), nodesStage2);
			}
			else {
				// Append a ".out" extension to the input filenames to form the output filenames

				Filename[] outputFilenames = new Filename[numberReducers];

				for (int i = 0; i < inputs.length; i++) {
					outputFilenames[i] = FileHelper.getFileInformation(baseDirectory.getPath(), inputs[i] + ".out", baseDirectory.getProtocol());
				}

				mapReduceSpecification.insertReducers(outputFilenames, nodesStage2);
			}
		} catch (OverlapingFilesException exception) {
			System.err.println(exception);

			System.exit(1);
		}

		try {
			mapReduceSpecification.setupCommunication(useTCP);
		} catch (OverlapingFilesException exception) {
			System.err.println(exception);

			System.exit(1);
		}

		try {
			manager.registerApplication(mapReduceSpecification);
		} catch (RemoteException exception) {
			System.err.println("Unable to contact manager");
			exception.printStackTrace();

			System.exit(1);
		}
	}	

	public static void main(String[] arguments) {
		String registryLocation = System.getProperty("java.rmi.server.location");

		String baseDirectory = System.getProperty("hammr.client.basedir"); 

		String command = arguments[0];

		if (command.equals("test1")) {
			Client client = new Client(registryLocation, new Directory(baseDirectory));

			client.performTest1();

			System.exit(0);
		}

		if (command.equals("test2")) {
			Client client = new Client(registryLocation, new Directory(baseDirectory));

			client.performTest2(CommunicationMode.TCP);

			System.exit(0);
		}

		if (command.equals("test3")) {
			if (arguments.length <= 1) {
				System.err.println("Usage: Client test3 <number_nodes_edges>");

				System.exit(1);
			}

			Client client = new Client(registryLocation, new Directory(baseDirectory));

			int numberNodesEdges = Integer.parseInt(arguments[3]);

			client.performTest3(numberNodesEdges);

			System.exit(0);
		}

		if (command.equals("perform_mapreduce")) {
			if(arguments.length <= 3) {
				System.err.println("Usage: Client perform_mapreduce <useTCP> <performMerge> [<input_filename> ... <input_filename>]");
				System.err.println("<useTCP> \"true\" or \"false\""); 
				System.err.println("<performMerge> \"true\" or \"false\"");

				System.exit(1);
			}

			Client client = new Client(registryLocation, new Directory(baseDirectory));

			boolean useTCP = false;

			if (arguments[1].equals("true")) {
				useTCP = true;
			}
			else if (arguments[1].equals("false")) {
				useTCP = false;
			}
			else {
				System.err.println("<useTCP> \"true\" or \"false\""); 

				System.exit(1);
			}

			boolean performMerge = false;

			if (arguments[2].equals("true")) {
				performMerge = true;
			}
			else if (arguments[2].equals("false")) {
				performMerge = false;
			}
			else {
				System.err.println("<performMerge> \"true\" or \"false\"");

				System.exit(1);
			}

			List<String> temporaryInputFilenames = new ArrayList<String>();

			for (int i = 3; i < arguments.length; i++) {
				temporaryInputFilenames.add(arguments[i]);
			}

			String[] finalInputFilenames = temporaryInputFilenames.toArray(new String[temporaryInputFilenames.size()]);

			client.performMapReduce(finalInputFilenames, useTCP, false, performMerge);
		}
	}
}
