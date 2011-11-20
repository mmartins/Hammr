package appspecs;

import java.io.EOFException;

import java.io.IOException;
import java.io.Serializable;

import java.util.Collection;

import java.util.Set;
import java.util.Map;
import java.util.HashMap;

import communication.ChannelHandler;
import communication.Record;

import execinfo.NodeGroup;
import execinfo.ProgressReport;

import utilities.RecordReaderShuffler;
import utilities.RecordWriterShuffler;

import utilities.MutableInteger;

public abstract class Node implements Serializable, Runnable {
	private static final long serialVersionUID = 1L;

	protected String name;

	protected NodeType type;

	protected Map<String, ChannelHandler> inputs;
	protected Map<String, ChannelHandler> outputs;

	protected MutableInteger mark;

	protected NodeGroup nodeGroup;

	protected ProgressReport progressReport;
	
	protected RecordReaderShuffler readersShuffler;
	protected RecordWriterShuffler writersShuffler;

	public Node(String name, NodeType type) {
		setType(type);

		inputs = new HashMap<String, ChannelHandler>();
		outputs = new HashMap<String, ChannelHandler>();
		progressReport = new ProgressReport();
	}

	public Node() {
		this(null, NodeType.COMMON);
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setType(NodeType type) {
		this.type = type;
	}

	public NodeType getType() {
		return type;
	}

	/* INPUT getters/adders */

	public Set<String> getInputChannelNames() {
		return inputs.keySet();
	}

	public Collection<ChannelHandler> getInputChannels() {
		return inputs.values();
	}

	public void addInputChannel(ChannelHandler input) {
		inputs.put(input.getName(), input);
	}

	public ChannelHandler getInputChannel(String source) {
		return inputs.get(source);
	}

	/* OUTPUT getters/adders */

	public Set<String> getOutputChannelNames() {
		return outputs.keySet();
	}

	public Collection<ChannelHandler> getOutputChannels() {
		return outputs.values();
	}

	public void addOutputChannel(ChannelHandler output) {
		outputs.put(output.getName(), output);
	}

	public ChannelHandler getOutputChannel(String target) {
		return outputs.get(target);
	}

	/* Read Functions */

	public Record readChannel(String channelName) {
		ChannelHandler channelHandler = getInputChannel(channelName);

		if (channelHandler != null) {
			try {
				return channelHandler.read();
			} catch (EOFException exception) {
				return null;
			} catch (IOException exception) {
				System.err.println("Error reading channel from node " + channelName + " to node " + this);

				exception.printStackTrace();
			}
		}

		System.err.println("Couldn't find channel handler " + channelName +  " for node " + this);

		return null;
	}

	public Record readArbitraryChannel() {
		if (readersShuffler == null) {
			createReaderShuffler();
		}

		try {
			return readersShuffler.readArbitrary();
		} catch (EOFException exception) {
			return null;
		} catch (IOException exception) {
			System.err.println("Error reading from arbitrary channel from node " + this);

			exception.printStackTrace();
		}

		return null;
	}

	/* Write Functions */

	public boolean writeChannel(Record record, String channelName) {
		ChannelHandler channelHandler = getOutputChannel(channelName);

		if (channelHandler != null) {
			try {
				channelHandler.write(record);

				return true;
			} catch (IOException exception) {
				System.err.println("Error writing channel to node " + channelName +  " from node " + this);

				exception.printStackTrace();
				return false;
			}
		}

		System.err.println("Couldn't find channel handler " + channelName +  " for node " + this);

		return false;
	}

	public boolean writeArbitraryChannel(Record record) {
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

	public boolean writeAllChannels(Record record) {
		Set<String> outputChannelNames = getOutputChannelNames();

		boolean finalResult = true;

		for (String outputChannelName: outputChannelNames) {
			boolean immediateResult = writeChannel(record, outputChannelName);

			if (immediateResult == false) {
				System.err.println("Error writing to all channel elements (error on channel " + outputChannelName + ") from node " + this);
			}

			finalResult |= immediateResult;
		}

		return finalResult;
	}

	/* Close functions */

	public boolean closeOutputs() {
		Collection<ChannelHandler> outputChannelHandlers = getOutputChannels();

		return closeChannels(outputChannelHandlers);
	}

	public boolean closeChannels(Collection<ChannelHandler> channelHandlers) {
		boolean finalResult = true;

		for (ChannelHandler channelHandler: channelHandlers) {
			try {
				boolean immediateResult = channelHandler.close();

				if (immediateResult == false) {
					System.err.println("Error closing channel " + channelHandler.getName() + " for node " + this);
				}

				finalResult |= immediateResult;
			} catch (IOException exception) {
				System.err.println("Error closing channel " + channelHandler.getName() + " for node " + this + " (I/O error)");

				exception.printStackTrace();
				finalResult |= false;
			}
		}

		return finalResult;
	}

	/* ReaderShuffler and WriterShuffler functions */

	private void createReaderShuffler() {
		try {
			readersShuffler = new RecordReaderShuffler(inputs);
		} catch (IOException exception) {
			System.err.println("Error creating read shuffler for node " + this);

			exception.printStackTrace();
		}
	}

	private void createWriterShuffler() {
		Collection<ChannelHandler> channelHandlers = getOutputChannels();

		writersShuffler = new RecordWriterShuffler(channelHandlers);
	}

	/* Marking functions */

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
	
	/* Run & print functions */

	public abstract void run();

	public String toString() {
		return name;
	}
}
