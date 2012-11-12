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

import java.io.Serializable;

import interfaces.DVFS;
import utilities.dvfs.FreqStats;

public class X86_DVFS implements DVFS,Serializable {

	private static final long serialVersionUID = 1L;

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
	private int numCPUs;

	public X86_DVFS() {
		numCPUs = Runtime.getRuntime().availableProcessors();
	}
	
	/**
	 * Return number of CPUs (cores) of node
	 * @return number of CPUs
	 */
	public int getNumCPUs() {
		return numCPUs;
	}
	
	/**
	 * Return latency time for state transition for all active CPUs
	 * @return transition latencies for each CPU (in 10 ms)
	 */
	public long[] getTransitionLatencies() {
		long[] latencies = new long[numCPUs];
		
		for (int i = 0; i < latencies.length; i++) {
			latencies[i] = getTransitionLatency(i);
		}
		
		return latencies;
	}
	
	/**
	 * Returns current frequency (in Hz) for each CPU (core)
	 * @return frequencies of all CPUs
	 */
	public long[] getFrequencies() {
		long[] frequencies = new long[numCPUs];
		
		for (int i = 0; i < frequencies.length; i++) {
			frequencies[i] = getFrequency(i);
		}
		
		return frequencies;
	}
	
	/**
	 * Return set of available frequencies (Hz) for each CPU (core)
	 * @return set of available DVFS frequencies for each CPU
	 */
	public long[][] getAvailableFrequencies() {
		long[][] availFreqs = new long[numCPUs][];
		
		for (int i = 0; i < availFreqs.length; i++) {
			availFreqs[i] = getAvailableFrequencies(i);
		}
		return availFreqs;
	}
	
	/**
	 * Sets frequencies for all CPUs (cores)
	 * @param frequencies Frequency to be set (Hz) for each core
	 * @return whether frequency set worked or not
	 */
	public boolean setFrequencies(long[] frequencies) {
		boolean ret = true;
		
		for (int i = 0; i < numCPUs; i++) {
			ret |= setFrequency(i, frequencies[i]) == 0 ? false : true;
		}
		
		return ret;
	}
	
	/**
	 * Returns frequency and time of stay for each CPU (core)
	 * @returns frequency statistics (Hz, 10 ms) for each core
	 */
	public FreqStats[] getFreqStats() {
		FreqStats[] stats = new FreqStats[numCPUs];
		
		for (int i = 0; i < stats.length; i++) {
			stats[i] = getFreqStats(i);
		}
		
		return stats;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		X86_DVFS dvfs = new X86_DVFS();
		int numCPUs = dvfs.getNumCPUs();
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
		System.load("/home/martins/git/Hammr/Common/bin/platforms/x86/libx86DVFS.so");
	}

}
