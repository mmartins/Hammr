/*
Copyright (c) 2011, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package appspecs;

import java.io.Serializable;

import java.util.Map;

import interfaces.ApplicationAggregator;

public abstract class Decider implements Serializable {
	private static final long serialVersionUID = 1L;

	protected ApplicationSpecification applicationSpecification;

	protected boolean requiresRunning;

	protected Map<String, ApplicationAggregator<? extends Serializable, ? extends Serializable>> aggregatedVariables;

	public Decider(ApplicationSpecification applicationSpecification) {
		this.applicationSpecification = applicationSpecification;

		this.requiresRunning = true;
	}

	public boolean requiresRunning() {
		return requiresRunning;
	}

	public void decideFollowingIteration(Map<String, ApplicationAggregator<? extends Serializable, ? extends Serializable>> aggregatedVariables) {
		this.aggregatedVariables = aggregatedVariables;

		decideFollowingIteration();
	}

	protected abstract void decideFollowingIteration();
}
