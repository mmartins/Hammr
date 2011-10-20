package utilities.counting;

import java.io.IOException;

import utilities.OutputExtractor;

import communication.Record;
import mapreduce.communication.MRRecord;

public class CountingOutputExtractor extends OutputExtractor {
	public CountingOutputExtractor(String[] inputOutputPairs) {
		super(inputOutputPairs);
	}

	public CountingOutputExtractor(String[] inputs, String[] outputs) {
		super(inputs, outputs);
	}

	protected String obtainInformation(Record genericRecord) {
		@SuppressWarnings("unchecked")
		MRRecord<String,Long> record = (MRRecord<String,Long>) genericRecord;

		return (record.getObject()) +  " - " + record.getValue() + "\n";
	}

	public static void main(String[] arguments) {
		CountingOutputExtractor extractor = new CountingOutputExtractor(arguments);

		try {
			extractor.run();
		} catch (IOException exception) {
			System.err.println("Error generating input");

			exception.printStackTrace();
		}
	}
}
