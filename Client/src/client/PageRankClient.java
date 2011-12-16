package client;

import enums.CommunicationMode;
import exceptions.OverlapingFilesException;
import graphs.appspecs.GraphProcessingSpecification;
import graphs.programs.GraphEdge;
import graphs.programs.GraphVertex;
import graphs.programs.pagerank.AbstractPagerankWorker;
import graphs.programs.pagerank.ControllerPagerankWorker;
import graphs.programs.pagerank.DeciderFilePagerankWorker;
import graphs.programs.pagerank.DeciderTCPPagerankWorker;
import graphs.programs.pagerank.PagerankController;
import graphs.programs.pagerank.PagerankDecider;
import graphs.programs.pagerank.PagerankFinishAggregator;

import interfaces.Manager;

import java.rmi.RemoteException;
import java.util.Arrays;

import utilities.RMIHelper;
import utilities.filesystem.Directory;
import utilities.filesystem.FileHelper;
import utilities.filesystem.Filename;
import appspecs.Node;

public class PageRankClient {
	private String registryLocation;

	private Directory baseDirectory;

	public PageRankClient(String registryLocation, Directory baseDirectory) {
		this.registryLocation = registryLocation;

		this.baseDirectory = baseDirectory;
	}

	public void runDeciderFilePagerank(String[] graphInputs, String[] pagerankInputs, int numberVertexes) {
		int numberWorkers = graphInputs.length;

		Manager manager = (Manager) RMIHelper.locateRemoteObject(registryLocation, "Manager");

		GraphProcessingSpecification<GraphVertex,GraphEdge> graphProcessingSpecification = new GraphProcessingSpecification<GraphVertex,GraphEdge>("pagerank", baseDirectory);

		// Add the workers

		Node[] workers = new Node[numberWorkers];

		for(int i = 0; i < workers.length; i++) {
			workers[i] = new DeciderFilePagerankWorker(i, numberVertexes, numberWorkers);
//			workers[i] = new ControllerPagerankWorker(i, numberVertexes, numberWorkers);
		}

		graphProcessingSpecification.insertWorkers(workers);

		// Add the inputs and outputs

		try {
			for(int i = 0; i < workers.length; i++) {
				
				Filename graphInputFilename;
				Filename pagerankInputFilename;
				Filename outputFilename;
				
				graphInputFilename = FileHelper.getFileInformation(baseDirectory.getPath(), graphInputs[i], baseDirectory.getProtocol());
				pagerankInputFilename = FileHelper.getFileInformation(baseDirectory.getPath(), pagerankInputs[i], baseDirectory.getProtocol());
				outputFilename = FileHelper.getFileInformation(baseDirectory.getPath(), pagerankInputs[i] + ".out", baseDirectory.getProtocol());

				graphProcessingSpecification.addInput(workers[i], graphInputs[i], graphInputFilename);
				graphProcessingSpecification.addInput(workers[i], pagerankInputs[i], pagerankInputFilename);
				graphProcessingSpecification.addOutput(workers[i], pagerankInputs[i] + ".out", outputFilename);
			}
			
			// Add empty message files as inputs
			for(int i = 0; i < workers.length; ++i)
			{
				for(int j = 0; j < workers.length; ++j)
				{			
					Filename inputFilename = FileHelper.getFileInformation(baseDirectory.getPath(),
							AbstractPagerankWorker.messageInputFilename(j, i), baseDirectory.getProtocol());
					Filename outputFilename = FileHelper.getFileInformation(baseDirectory.getPath(),
							AbstractPagerankWorker.messageOutputFilename(i, j), baseDirectory.getProtocol());
					graphProcessingSpecification.addInput(workers[i], AbstractPagerankWorker.messageInputFilename(
							j, i), inputFilename);
					graphProcessingSpecification.addOutput(workers[i], AbstractPagerankWorker
							.messageOutputFilename(i, j), outputFilename);
				}
			}
			
		} catch (OverlapingFilesException exception) {
			System.err.println(exception);

			System.exit(1);
		}
	
		
		// Add pairwise TCP communication among the workers

		//graphProcessingSpecification.insertEdges(workers, workers, CommunicationMode.TCP);

		try {
			graphProcessingSpecification.finalize();
		} catch (OverlapingFilesException exception) {
			System.err.println(exception);

			System.exit(1);
		}

		// Add a controller to permit workers detect the end of the iterative processes

		graphProcessingSpecification.addAggregator("finish", new PagerankFinishAggregator());
		graphProcessingSpecification.setDecider(new PagerankDecider(graphProcessingSpecification));
		
		try {
			manager.registerApplication(graphProcessingSpecification);
		} catch (RemoteException exception) {
			System.err.println("Unable to contact manager");

			System.exit(1);
		}
	}

	
	public void runControllerPagerank(String[] graphInputs, String[] pagerankInputs, int numberVertexes) {
		int numberWorkers = graphInputs.length;

		Manager manager = (Manager) RMIHelper.locateRemoteObject(registryLocation, "Manager");

		GraphProcessingSpecification<GraphVertex,GraphEdge> graphProcessingSpecification = new GraphProcessingSpecification<GraphVertex,GraphEdge>("pagerank", baseDirectory);

		// Add the workers

		Node[] workers = new Node[numberWorkers];

		for(int i = 0; i < workers.length; i++) {
			workers[i] = new ControllerPagerankWorker(i, numberVertexes, numberWorkers);
		}

		graphProcessingSpecification.insertWorkers(workers);

		// Add the inputs and outputs

		try {
			for(int i = 0; i < workers.length; i++) {
				
				Filename graphInputFilename;
				Filename pagerankInputFilename;
				Filename outputFilename;
				
				graphInputFilename = FileHelper.getFileInformation(baseDirectory.getPath(), graphInputs[i], baseDirectory.getProtocol());
				pagerankInputFilename = FileHelper.getFileInformation(baseDirectory.getPath(), pagerankInputs[i], baseDirectory.getProtocol());
				outputFilename = FileHelper.getFileInformation(baseDirectory.getPath(), pagerankInputs[i] + ".out", baseDirectory.getProtocol());

				graphProcessingSpecification.addInput(workers[i], graphInputs[i], graphInputFilename);
				graphProcessingSpecification.addInput(workers[i], pagerankInputs[i], pagerankInputFilename);
				graphProcessingSpecification.addOutput(workers[i], pagerankInputs[i] + ".out", outputFilename);
			}
			
		} catch (OverlapingFilesException exception) {
			System.err.println(exception);

			System.exit(1);
		}
	
		
		// Add pairwise TCP communication among the workers

		graphProcessingSpecification.insertEdges(workers, workers, CommunicationMode.TCP);

		try {
			graphProcessingSpecification.finalize();
		} catch (OverlapingFilesException exception) {
			System.err.println(exception);

			System.exit(1);
		}

		// Add a controller to permit workers detect the end of the iterative processes
		
		graphProcessingSpecification.addController("finish", new PagerankController(numberWorkers));
		
		try {
			manager.registerApplication(graphProcessingSpecification);
		} catch (RemoteException exception) {
			System.err.println("Unable to contact manager");

			System.exit(1);
		}
	}
	
	
	public static void main(String[] arguments) {
		String registryLocation = System.getProperty("java.rmi.server.location");

		String baseDirectory = System.getProperty("hammr.client.basedir"); 

		if(arguments.length <= 1) {
			System.err.println("Usage: Client <number_vertexes> [<graph_filename> ... <graph_filename>] [<pagerank_filename> ... <pagerank_filename>]");

			System.exit(1);
		}

		int numberVertexes = Integer.valueOf(arguments[0]);
		int numberWorkers = (arguments.length - 1)/2;

		String[] graphInputs = Arrays.copyOfRange(arguments, 1, 1+numberWorkers);
		String[] pagerankInputs = Arrays.copyOfRange(arguments, 1+numberWorkers, 1+2*numberWorkers);

		PageRankClient client = new PageRankClient(registryLocation, new Directory(baseDirectory));
		client.runDeciderFilePagerank(graphInputs, pagerankInputs, numberVertexes);
	}
}
