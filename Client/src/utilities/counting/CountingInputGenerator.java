package utilities.counting;

import java.io.BufferedReader;

import java.io.IOException;

import java.util.Set;
import java.util.HashSet;

import utilities.InputGenerator;

import communication.Record;
import mapreduce.communication.MRRecord;

public class CountingInputGenerator extends InputGenerator {
	public CountingInputGenerator(String[] inputOutputPairs) {
		super(inputOutputPairs);
	}

	public CountingInputGenerator(String[] inputs, String[] outputs) {
		super(inputs, outputs);
	}

	protected String obtainBuffer(BufferedReader reader) throws IOException {
		return reader.readLine();
	}

	protected Set<Record> generateInput(String buffer) {
		Set<Record> result = new HashSet<Record>();

		String delimiters = "[^\\w]+";

		String[] words = buffer.split(delimiters);

		for (String word: words) {
			result.add(new MRRecord<String,Long>(word.toLowerCase(), 0L));
		}

		return result;
	}

	public static void main(String[] arguments) {
		CountingInputGenerator generator = new CountingInputGenerator(arguments);

		try {
			generator.run();
		} catch (IOException exception) {
			System.err.println("Error generating input");

			exception.printStackTrace();
		}
	}
}
