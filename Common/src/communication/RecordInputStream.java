package communication;

import java.io.IOException;
import java.io.EOFException;

import java.io.InputStream;
import java.io.ObjectInputStream;

public class RecordInputStream extends ObjectInputStream {
	public RecordInputStream(InputStream inputStream) throws IOException {
		super(inputStream);
	}

	public Record readRecord() throws EOFException, IOException {
		try {
			return (Record) readObject();
		} catch (ClassNotFoundException exception) {
			System.err.println("Error reading from channel: unknown class");

			exception.printStackTrace();

			return null;
		}
	}
}
