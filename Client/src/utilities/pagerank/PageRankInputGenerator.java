package utilities.pagerank;

import graphs.programs.pagerank.AbstractPagerankWorker;
import utilities.filesystem.Directory;
import utilities.filesystem.FileHelper;
import utilities.filesystem.Filename;

import communication.writers.FileChannelElementWriter;

import mapreduce.communication.MRChannelElement;

public class PageRankInputGenerator {
	public static void main(String[] args) throws Exception
	{
		if(args.length < 3) {
			System.err.println("usage: FileGraphInputGenerator baseDirectory numberVertices numberWorkers");

			System.exit(1);
		}
		
		String baseDir = args[0];
		Directory dir = new Directory(baseDir);
		int numVertices = Integer.parseInt(args[1]);
		int numWorkers = Integer.parseInt(args[2]);
		
		Filename[] outputs = new Filename[numWorkers];
		for(int i = 0; i < numWorkers; ++i)
		{
			outputs[i] = FileHelper.getFileInformation(dir.getPath(), AbstractPagerankWorker.pagerankInputFilename(i), dir.getProtocol());
		}
		
		
		double initialPageRank = 1.0 / numVertices;
		int verticesPerNode = numVertices / outputs.length;

		FileChannelElementWriter writer = null;
		for(int i = 0, j = -1; i < numVertices; ++i)
		{
			if(i % verticesPerNode == 0)
			{
				if(writer != null) writer.close();
				writer = new FileChannelElementWriter(outputs[++j]);
			}
			
			writer.write(new MRChannelElement<Integer,Double>(i, initialPageRank));
		}
		
		writer.close();
		
		//generate empty message files
		for(int i = 0; i < numWorkers; ++i)
		{
			for(int j = 0; j < numWorkers; ++j)
			{			
				Filename fn = FileHelper.getFileInformation(dir.getPath(), AbstractPagerankWorker
						.messageInputFilename(i, j), dir.getProtocol());
				writer = new FileChannelElementWriter(fn);
				writer.close();
			}
		}
	}
}
