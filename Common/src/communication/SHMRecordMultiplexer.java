package communication;

import java.util.Collections;

import java.util.Set;
import java.util.HashSet;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import java.io.EOFException;
import java.io.IOException;

public class SHMRecordMultiplexer implements RecordReader {
	private static int DEFAULT_LIMIT = 32;
	private static int DEFAULT_RETRY = 250;

	protected Set<String> origins;

	protected BlockingQueue<Record> queue;

	public SHMRecordMultiplexer(Set<String> origins) {
		this.origins = Collections.synchronizedSet(new HashSet<String>());

		this.origins.addAll(origins);

		this.queue = new ArrayBlockingQueue<Record>(DEFAULT_LIMIT);
	}

	public Record read() throws EOFException, IOException {
		Record record;

		while (true) {
			try {
				record = queue.poll(DEFAULT_RETRY, TimeUnit.MILLISECONDS);

				if (record != null) {
					return record;
				}
				else if (origins.size() == 0) {
					throw new EOFException();
				}
			} catch (InterruptedException exception) {
				System.err.println("Unexpected thread interruption while waiting for a read");

				exception.printStackTrace();
			}
		}
	}

	public Record tryRead() throws EOFException, IOException {
		return queue.poll();
	}

	public boolean write(String origin, Record record) throws IOException {
		try {
			queue.put(record);
		} catch (InterruptedException exception) {
			System.err.println("Unexpected thread interruption while waiting for write");

			exception.printStackTrace();
			return false;
		}

		return true;
	}

	public void close(String origin) {
		boolean result = origins.remove(origin);

		if (result == false) {
			System.err.println("Error deleting origin " + origin + " for SHM channel multiplexer");
		}
	}

	public void close() throws IOException {
		System.err.println("Closing without specifying the input is not permitted");

		throw new IOException();
	}
}
