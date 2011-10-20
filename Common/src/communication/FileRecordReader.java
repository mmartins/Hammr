package communication;

import java.io.FileInputStream;

import java.io.FileNotFoundException;
import java.io.EOFException;
import java.io.IOException;

public class FileRecordReader implements RecordReader {
	private RecordInputStream recordInputStream;

	public FileRecordReader(String location) throws FileNotFoundException, IOException {
		recordInputStream = new RecordInputStream(new FileInputStream(location));
	}

	public synchronized Record read() throws EOFException, IOException {
		return recordInputStream.readRecord();
	}

	public synchronized Record tryRead() throws EOFException, IOException {
		if (recordInputStream.available() > 0) {
			return read();
		}

		return null;
	}

	public void close() throws IOException {
		recordInputStream.close();
	}
}
