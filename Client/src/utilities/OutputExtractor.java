/*
Copyright (c) 2011, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package utilities;

import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import utilities.filesystem.Directory;
import utilities.filesystem.FileHelper;
import utilities.filesystem.Filename;

import communication.channel.Record;
import communication.readers.FileRecordReader;

public class OutputExtractor {
	private Filename[] inputs;
	private Filename[] outputs;

	public OutputExtractor(Directory directory, String[] inputsOutputs) {
		List<Filename> inputList = new ArrayList<Filename>();
		List<Filename> outputList = new ArrayList<Filename>();

		boolean foundColumn = false;

		for (int i = 0; i < inputsOutputs.length; i++) {
			if (inputsOutputs[i].equals(":")) {
				foundColumn = true;

				continue;
			}

			if (!foundColumn) {
				inputList.add(FileHelper.getFileInformation(directory.getPath(), inputsOutputs[i], directory.getProtocol()));
			}
			else {
				outputList.add(FileHelper.getFileInformation(directory.getPath(), inputsOutputs[i], directory.getProtocol()));
			}
		}	

		if (inputList.size() == 0 || outputList.size() == 0) {
			System.err.println("Parameters: <input> ... <input> : <output> ... <output>");

			System.exit(1);
		}

		this.inputs = inputList.toArray(new Filename[inputList.size()]);
		this.outputs = outputList.toArray(new Filename[outputList.size()]);
	}

	public OutputExtractor(Filename[] inputs, Filename[] outputs) {
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
			writers[i] = new BufferedWriter(new FileWriter(outputs[i].getLocation(), true));
		}

		int writerCount = 0;
		
		for (int i = 0; i < readers.length; i++) {
			Record record;

			while (true) {
				try {
					record = readers[i].read();
				} catch(EOFException exception) {
					break;
				}

				writers[(writerCount++) % writers.length].write(getData(record));
			}
		}

		for (int i = 0; i < readers.length; i++) {
			readers[i].close();
		}

		for (int i = 0; i < writers.length; i++) {
			writers[i].close();
		}
	}

	protected String getData(Record record) {
		return record.toString();
	}
}
