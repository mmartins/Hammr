package graphs.programs.pagerank;

import utilities.filesystem.FileHelper;
import utilities.filesystem.Filename;

import appspecs.ApplicationSpecification;
import appspecs.Decider;

public class PagerankDecider extends Decider{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	int counter = 1;
	
	public PagerankDecider(ApplicationSpecification applicationSpecification) {
		super(applicationSpecification);
	}


	@Override
	protected void decideFollowingIteration() {	
		try {
//			ApplicationAggregator<Boolean, Boolean> finishAggregator = (ApplicationAggregator<Boolean, Boolean>) aggregatedVariables
//					.get("finish");
//			
//			if(finishAggregator.obtainAggregate())
//				counter--;
			
			counter--;
			
			if(counter < 0)
			{
				requiresRunning = false;
			}
			else
			{
				requiresRunning = true;
				
				// nodenum == initialsnum
				
				for(Filename output : applicationSpecification.getOutputFilenames())
				{
					String outputLocation = output.getLocation();
					String inputLocation = outputLocation.substring(0, outputLocation.length() - 4);
					Filename input = new Filename(inputLocation, output.getProtocol());
					FileHelper.move(output, input);
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
