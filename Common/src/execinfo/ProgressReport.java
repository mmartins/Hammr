package execinfo;

/**
 * @author Marcelo Martins (martins)
 *
 */
public class ProgressReport {

	private double progress;
	
	public ProgressReport() {
		progress = 0.0;
	}
	
	public double getProgress() {
		return progress;
	}
	
	public void setProgress(double progress) {
		assert progress > 0.0 && progress < 1.0;
		
		this.progress = progress;
	}
}
