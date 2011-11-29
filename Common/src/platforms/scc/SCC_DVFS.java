/*
Copyright (c) 2011, Marcelo Martins

All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

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
