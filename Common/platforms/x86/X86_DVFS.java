/*

Copyright (c) 2011, Marcelo Martins

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.  Redistributions in binary
form must reproduce the above copyright notice, this list of conditions and the
following disclaimer in the documentation and/or other materials provided with
the distribution.  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.

*/

package platforms.x86;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class X86_DVFS {

	public native long getTransitionLatency(int cpu);
	public native int getHardwareLimits(int cpu);
	
	public native String getGovernor(int cpu);
	public native String[] getAvailableGovernors(int cpu);
	public native int setGovernor(int cpu, String governorName);
	
	public native long getFrequency(int cpu);
	public native long[] getAvailableFrequencies(int cpu);
	public native int setFrequency(int cpu, long freq);
	
	public native int setFreqPolicy(int cpu, long minFreq, long maxFreq, String policyName);
	public native FreqStats getFreqStats(int cpu);
	
	private long minFreq, maxFreq;

	
	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		X86_DVFS dvfs = new X86_DVFS();
		Runtime runtime = Runtime.getRuntime();
		int numCPUs = runtime.availableProcessors();
		FreqStats stats;

		System.out.println("Number of cores: " + numCPUs);
	    
		for (int i = 0; i < numCPUs; i++) {
			System.out.println("Current governor["+ i + "]: " + dvfs.getGovernor(i));
			System.out.println("Available governors: ");
			String[] govs =  dvfs.getAvailableGovernors(i);

			for (int j = 0; j < govs.length; j++)
				System.out.print(govs[j] + " ");
			System.out.println("\nCurrent frequency: " + dvfs.getFrequency(i));
			
			long[] freqs =  dvfs.getAvailableFrequencies(i);
			System.out.println("Available frequencies: ");
			for (int j = 0; j < freqs.length; j++)
				System.out.print(freqs[j] + " ");

			System.out.println("Stats: ");
			stats = dvfs.getFreqStats(i);

			System.out.println("Size stats: " + stats.size());
			for (int j = 0; j < freqs.length; j++) {
				System.out.print("Freq: " + freqs[j] + " ");
 				System.out.println("Time: " + stats.getDuration(freqs[j]));
			}
		}
	}
	
	static {
		System.load("/home/martins/git/Hammr/Common/platforms/x86/libx86DVFS.so");
	}

}
