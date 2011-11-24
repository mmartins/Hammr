package platforms.scc;

public class SCC_DVFS {

	public native int getPowerDomain();
	public native int getPowerDomainMaster();
	public native int getPowerDomainSize();
	
	public native double setPower(int freqDiv);
	public native int waitPower();
	public native int setFrequencyDivider(int freqDiv);
	
	public static void main(String[] args)
	{
		SCC scc = new SCC();
		SCC_DVFS dvfs = new SCC_DVFS();
		
		scc.init(args);
		System.out.println("Current power domain: " + dvfs.getPowerDomain());
		System.out.println("Power domain master: " + dvfs.getPowerDomainMaster());
		System.out.println("Power domain size: " + dvfs.getPowerDomainSize());
		scc.terminate();
	}
	
	static {
		System.load("scc");
		System.loadLibrary("sccDVFS");
	}
}
