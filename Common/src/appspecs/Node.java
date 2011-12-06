/*
Copyright (c) 2011, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package appspecs;

import java.util.concurrent.TimeUnit;

import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import platforms.x86.X86_Energy;

import utilities.MutableInteger;

import communication.channel.InputChannel;
import communication.channel.OutputChannel;
import communication.channel.Record;
import communication.shufflers.RecordReaderShuffler;
import communication.shufflers.RecordWriterShuffler;

import execinfo.NodeGroup;
import execinfo.ProgressReport;

public abstract class Node implements Serializable, Runnable {
	private static final long serialVersionUID = 1L;

	///////////////////////////////
	// SPECIFICATION INFORMATION //
	///////////////////////////////

	protected String name;

	protected Map<String, InputChannel> inputs;
	protected Map<String, OutputChannel> outputs;

	/* Runtime information */
	protected Map<String, InputChannel> structuralInputs;
	protected Map<String, OutputChannel> structuralOutputs;

	protected Map<String, InputChannel> applicationInputs;
	protected Map<String, OutputChannel> applicationOutputs;

	protected RecordReaderShuffler readersShuffler;
	protected RecordWriterShuffler writersShuffler;

	/////////////////////////
	// RUNNING INFORMATION //
	/////////////////////////

	protected NodeGroup nodeGroup;

	protected ProgressReport progressReport;
	
	protected Energy energy;

	/////////////////////////
	// PARSING INFORMATION //
	/////////////////////////

	protected MutableInteger mark;

	public Node() {
		this(null);
	}

	public Node(String name) {
		progressReport = new ProgressReport();
		energy = new X86_Energy();
		
		inputs = new HashMap<String, InputChannel>();
		outputs = new HashMap<String, OutputChannel>();

		applicationInputs = new HashMap<String, InputChannel>();
		applicationOutputs = new HashMap<String, OutputChannel>();

		structuralInputs = new HashMap<String, InputChannel>();
		structuralOutputs = new HashMap<String, OutputChannel>();
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	/* INPUT getters/adders */

	public Set<String> getInputChannelNames() {
		return inputs.keySet();
	}

	public Set<String> getApplicationInputChannelNames() {
		return applicationInputs.keySet();
	}

	public Set<String> getStrucutralInputChannelNames() {
		return applicationInputs.keySet();
	}

	public Collection<InputChannel> getInputChannels() {
		return inputs.values();
	}

	public Collection<InputChannel> getApplicationInputChannels() {
		return applicationInputs.values();
	}

	public Collection<InputChannel> getStructuralInputChannels() {
		return structuralInputs.values();
	}

	public void addInputChannel(String source, InputChannel input, boolean applicationInput) {
		inputs.put(source, input);

		if (applicationInput) {
			applicationInputs.put(source, input);
		}
		else {
			structuralInputs.put(source, input);
		}
	}

	public InputChannel delInputChannel(String source) {
		return inputs.remove(source);
	}

	public InputChannel getInputChannel(String source) {
		return inputs.get(source);
	}

	/* OUTPUT getters/adders */

	public Set<String> getOutputChannelNames() {
		return outputs.keySet();
	}

	public Set<String> getApplicationOutputChannelNames() {
		return applicationOutputs.keySet();
	}

	public Set<String> getStructuralOutputChannelNames() {
		return structuralOutputs.keySet();
	}

	public Collection<OutputChannel> getOutputChannels() {
		return outputs.values();
	}

	public Collection<OutputChannel> getApplicationOutputChannels() {
		return applicationOutputs.values();
	}

	public Collection<OutputChannel> getStructuralOutputChannels() {
		return structuralOutputs.values();
	}

	public void addOutputChannel(String target, OutputChannel output, boolean applicationOutput) {
		outputs.put(target, output);

		if (applicationOutput) {
			applicationOutputs.put(target, output);
		}
		else {
			structuralOutputs.put(target, output);
		}
	}

	public OutputChannel delOutputChannel(String target) {
		return outputs.remove(target);
	}

	public OutputChannel getOutputChannel(String target) {
		return outputs.get(target);
	}

	/* Read Functions */

	protected Record readChannel(String channelName) {
		InputChannel inputChannel = getInputChannel(channelName);

		if (inputChannel != null) {
			try {
				return inputChannel.read();
			} catch (EOFException exception) {
				return null;
			} catch (IOException exception) {
				System.err.println("Error reading channel from node " + channelName + " to node " + this);

				exception.printStackTrace();
			}
		}

		System.err.println("Couldn't find input channel " + channelName +  " for node " + this);

		return null;
	}

	protected Record readArbitraryChannel() {
		if (readersShuffler == null) {
			createReaderShuffler();
		}

		try {
			return readersShuffler.readArbitrary();
		} catch (EOFException exception) {
			return null;
		} catch (IOException exception) {
			System.err.println("Error reading record from node " + this);

			exception.printStackTrace();
		}

		return null;
	}

	protected Record tryReadArbitraryChannel() {
		// You need to create the read shuffler manually if you want to use this method

		if (readersShuffler == null) {
			throw new IllegalStateException();
		}

		try {
			return readersShuffler.tryReadArbitrary();
		} catch (IOException exception) {
			System.err.println("Error reading arbitrary record from node " + this);

			exception.printStackTrace();
		}

		return null;
	}

	protected Record tryReadArbitraryChannel(int timeout, TimeUnit timeUnit) {
		// You need to create the read shuffler manually if you want to use this method

		if (readersShuffler == null) {
			throw new IllegalStateException();
		}

		try {
			return readersShuffler.tryReadArbitrary(timeout, timeUnit);
		} catch (IOException exception) {
			System.err.println("Error reading arbitrary record from node " + this);

			exception.printStackTrace();
		}

		return null;
	}

	/* Write Functions */

	protected boolean writeChannel(Record record, String channelName) {
		OutputChannel outputChannel = getOutputChannel(channelName);

		if (outputChannel != null) {
			try {
				outputChannel.write(record);

				return true;
			} catch (IOException exception) {
				System.err.println("Error writing record to node " + channelName +  " from node " + this);

				exception.printStackTrace();
				return false;
			}
		}

		System.err.println("Couldn't find output channel " + name +  " for node " + this);

		return false;
	}

	protected boolean writeArbitraryChannel(Record record) {
		if (writersShuffler == null) {
			createWriterShuffler();
		}

		try {
			return writersShuffler.writeArbitrary(record);
		} catch (IOException exception) {
			System.err.println("Error writing to arbitary channel from node " + this);

			exception.printStackTrace();
		}

		return false;
	}

	protected boolean writeAllChannels(Record record) {
		Set<String> outputChannelNames = getOutputChannelNames();

		boolean finalResult = true;

		for (String outputChannelName: outputChannelNames) {
			boolean immediateResult = writeChannel(record, outputChannelName);

			if (immediateResult == false) {
				System.err.println("Error writing all records (error on channel " + outputChannelName + ") from node " + this);
			}

			finalResult |= immediateResult;
		}

		return finalResult;
	}

	/* Close functions */

	protected boolean closeOutputs() {
		Collection<OutputChannel> outputChannels = getOutputChannels();

		return closeOutputs(outputChannels);
	}

	protected boolean closeOutputs(Collection<OutputChannel> outputChannels) {
		boolean finalResult = true;

		for (OutputChannel channel: outputChannels) {
			try {
				boolean immediateResult = channel.close();

				if (immediateResult == false) {
					System.err.println("Error closing channel " + channel.getName() + " for node " + this);
				}

				finalResult |= immediateResult;
			} catch (IOException exception) {
				System.err.println("Error closing channel " + channel.getName() + " for node " + this + " (I/O error)");

				exception.printStackTrace();
				finalResult |= false;
			}
		}

		return finalResult;
	}

	/* ReaderShuffler and WriterShuffler functions */

	protected void createReaderShuffler() {
		try {
			readersShuffler = new RecordReaderShuffler(inputs);
		} catch (IOException exception) {
			System.err.println("Error creating read shuffler for node " + this);

			exception.printStackTrace();
		}
	}

	protected void createReaderShuffler(boolean structural, boolean application) {
		Map<String, InputChannel> selected = null;

		if (structural && application) {
			selected = inputs;
		}

		if (structural && !application) {
			selected = structuralInputs;
		}

		if (!structural && application) {
			selected = applicationInputs;
		}

		try {
			readersShuffler = new RecordReaderShuffler(selected);
		} catch (IOException exception) {
			System.err.println("Error creating read shuffler for node " + this);

			exception.printStackTrace();
		}
	}

	protected void createWriterShuffler() {
		Collection<OutputChannel> outputChannels = getOutputChannels();

		writersShuffler = new RecordWriterShuffler(outputChannels);
	}

	/* Running functions */

	public void prepareSchedule() {
		setMark(null);
	}

	/* Parsing functions */

	public MutableInteger getMark() {
		return mark;
	}

	public void setMark(MutableInteger mark) {
		if (!isMarked() || mark == null) {
			this.mark = mark;
		}
		else {
			this.mark.setValue(mark.getValue());
		}
	}

	public boolean isMarked() {
		return (mark != null);
	}

	/* NodeGroup functions */

	public void setNodeGroup(NodeGroup nodeGroup) {
		this.nodeGroup = nodeGroup;
	}

	public NodeGroup getNodeGroup() {
		return nodeGroup;
	}

	public ProgressReport getProgressReport() {
		return progressReport;
	}
	
	public void setProgressReport(ProgressReport progressReport) {
		this.progressReport.setProgress(progressReport.getProgress());
	}

	public long getEnergy() {
		return energy.getEnergy();
	}
	
	/* Run & print functions */

	public abstract void run();

	public String toString() {
		return name;
	}
}
