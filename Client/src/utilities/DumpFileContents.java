package utilities;

import java.io.FileNotFoundException;
import java.io.IOException;

import communication.Record;
import communication.FileRecordReader;

public class DumpFileContents {
	private String filename;

	public DumpFileContents(String filename) {
		this.filename = filename;
	}

	public void dump() throws FileNotFoundException, IOException {
		FileRecordReader reader = new FileRecordReader(filename);

		Record element;

		while ((element = reader.read()) != null) {
			System.out.println(element);
		}
	}
}
