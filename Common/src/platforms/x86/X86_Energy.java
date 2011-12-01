/*
Copyright (c) 2011, Marcelo Martins
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package platforms.x86;

import java.util.Iterator;

import appspecs.Energy;
import utilities.dvfs.FreqStats;

public class X86_Energy implements Energy {
	private X86_DVFS dvfs;
	
	public X86_Energy() {
		dvfs = new X86_DVFS();
	}
	
	public long getEnergy() {
		Runtime runtime = Runtime.getRuntime();
		int numCPUs = runtime.availableProcessors();
		long energy = 0L;

		FreqStats[] stats = new FreqStats[numCPUs];
		
		for (int i = 0; i < numCPUs; i++) {
			stats[i] = dvfs.getFreqStats(i);

			Iterator<Long> iter;
			iter = stats[i].getFreqs().iterator();
			
			/* TODO: Fix the formula below */
			while (iter.hasNext()) {
				long freq = iter.next();
				energy += freq * stats[i].getDuration(freq);
			}
		}
		
		return energy;
	}
}
