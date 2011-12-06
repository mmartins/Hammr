/*
Copyright (c) 2011, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package utilities;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import utilities.filesystem.Directory;
import utilities.filesystem.FileHelper;
import utilities.filesystem.Filename;

import communication.channel.Record;
import communication.writers.FileRecordWriter;

public class InputGenerator {
	private Filename[] inputs;
	private Filename[] outputs;

	public InputGenerator(Directory directory, String[] inputsOutputs) {
		List<Filename> inputList = new ArrayList<Filename>();
		List<Filename> outputList = new ArrayList<Filename>();

		boolean foundColum = false;

		for (int i = 0; i < inputsOutputs.length; i++) {
			if (inputsOutputs[i].equals(":")) {
				foundColum = true;

				continue;
			}

			if(!foundColum) {
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

	public InputGenerator(Filename[] inputs, Filename[] outputs) {
		this.inputs = inputs;
		this.outputs = outputs;
	}

	public void run() throws IOException {
		BufferedReader[] readers = new BufferedReader[inputs.length];

		for (int i = 0; i < inputs.length; i++) {
			readers[i] = new BufferedReader(new FileReader(inputs[i].getLocation()));
		}
		
		FileRecordWriter[] writers = new FileRecordWriter[outputs.length];
		
		for (int i = 0; i < outputs.length; i++) {
			writers[i] =  new FileRecordWriter(outputs[i]);
		}

		int writerCount = 0;
		
		for (int i = 0; i < readers.length; i++) {
			String buffer;

			while (true) {
				buffer = obtainBuffer(readers[i]);

				if (buffer == null) {
					break;
				}

				Set<Record> records = generateInput(buffer);

				for(Record record: records) {
					writers[(writerCount++) % writers.length].write(record);
				}
			}
		}

		for (int i = 0; i < readers.length; i++) {
			readers[i].close();
		}
		
		for (int i = 0; i < writers.length; i++) {
			writers[i].close();
		}
	}

	protected String obtainBuffer(BufferedReader reader) throws IOException {
		return reader.readLine();
	}

	protected Set<Record> generateInput(String buffer) {
		Set<Record> result = new HashSet<Record>();

		result.add(new Record(buffer, null));

		return result;
	}
}
