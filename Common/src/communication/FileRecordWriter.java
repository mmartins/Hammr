package communication;

import java.io.FileOutputStream;

import java.io.IOException;

public class FileRecordWriter implements RecordWriter {
	private RecordOutputStream recordOutputStream;

	public FileRecordWriter(String location) throws IOException {
		recordOutputStream = new RecordOutputStream(new FileOutputStream(location));
	}

	public synchronized boolean write(Record record) throws IOException {
		recordOutputStream.writeRecord(record);

		return true;
	}

	public synchronized boolean flush() throws IOException {
		recordOutputStream.flush();
		recordOutputStream.reset();

		return true;
	}

	public synchronized boolean close() throws IOException {
		recordOutputStream.flush();

		recordOutputStream.close();

		return true;
	}
}
