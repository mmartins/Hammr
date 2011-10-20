package utilities;

import java.util.ArrayList;
import java.util.List;

import java.io.BufferedWriter;
import java.io.FileWriter;

import java.io.EOFException;
import java.io.IOException;

import communication.Record;
import communication.FileRecordReader;

public abstract class OutputExtractor {
	private String[] inputs;
	private String[] outputs;

	public OutputExtractor(String[] inputsOutputs) {
		List<String> inputList = new ArrayList<String>();
		List<String> outputList = new ArrayList<String>();

		boolean foundColumn = false;

		for (int i = 0; i < inputsOutputs.length; i++) {
			if (inputsOutputs[i].equals(":")) {
				foundColumn = true;

				continue;
			}

			if (!foundColumn) {
				inputList.add(inputsOutputs[i]);
			}
			else {
				outputList.add(inputsOutputs[i]);
			}
		}	

		if (inputList.size() == 0 || outputList.size() == 0) {
			System.err.println("Parameters: <input> ... <input> : <output> ... <output>");

			System.exit(1);
		}

		if (inputList.size() != outputList.size()) {
			System.err.println("The specified inputs and outputs should have the same size");

			System.exit(1);
		}

		this.inputs = inputList.toArray(new String[inputList.size()]);
		this.outputs = outputList.toArray(new String[outputList.size()]);
	}

	public OutputExtractor(String[] inputs, String[] outputs) {
		this.inputs = inputs;
		this.outputs = outputs;
	}

	public void run() throws IOException {
		FileRecordReader[] readers = new FileRecordReader[inputs.length];

		for (int i = 0; i < inputs.length; i++) {
			readers[i] = new FileRecordReader(inputs[i]);
		}

		BufferedWriter[] writers = new BufferedWriter[outputs.length];

		for (int i = 0; i < outputs.length; i++) {
			writers[i] = new BufferedWriter(new FileWriter(outputs[i], true));
		}

		for (int i = 0; i < readers.length; i++) {
			Record record;

			while (true) {
				try {
					record = readers[i].read();
				} catch(EOFException exception) {
					break;
				}

				writers[i].write(obtainInformation(record));
			}
		}

		for (int i = 0; i < readers.length; i++) {
			readers[i].close();
		}

		for (int i = 0; i < writers.length; i++) {
			writers[i].close();
		}
	}

	protected abstract String obtainInformation(Record record);
}
