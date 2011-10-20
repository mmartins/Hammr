package communication;

import java.io.IOException;

import java.io.OutputStream;
import java.io.ObjectOutputStream;

public class RecordOutputStream extends ObjectOutputStream {
	private static long DEFAULT_WRITE_COUNT_FLUSH = 65535;

	private long writeCounter = 0L;

	public RecordOutputStream(OutputStream outputStream) throws IOException {
		super(outputStream);
	}

	public void writeRecord(Record channelElement) throws IOException {
		writeCounter++;

		if ((writeCounter % DEFAULT_WRITE_COUNT_FLUSH) == 0) {
			flush();
			reset();
		}

		writeObject(channelElement);
	}
}
