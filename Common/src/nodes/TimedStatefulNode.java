/*
Copyright (c) 2011, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package nodes;

import java.util.concurrent.TimeUnit;

import communication.channel.Record;

public abstract class TimedStatefulNode extends StatefulNode {
	private static final long serialVersionUID = 1L;

	protected int timeout;

	protected TimeUnit timeUnit;

	public TimedStatefulNode(int timeout, TimeUnit timeUnit) {
		this.timeout = timeout;

		this.timeUnit = timeUnit;

		this.terminate = false;
	}

	protected Record read() {
		return tryReadArbitraryChannel(timeout, timeUnit);
	}
	
	@Override
	public void run() {
		if (!performInitialization()) {
			return;
		}

		Record record;

		while (true) {
			
			record = tryReadArbitraryChannel(timeout, timeUnit);

			if(record == null) {
				performActionNothingPresent();
			}
			else {
				performAction(record);
			}

			if(terminate) {
				break;
			}
		}

		performTermination();

		shutdown();		
	}

	protected abstract void performActionNothingPresent();
}
