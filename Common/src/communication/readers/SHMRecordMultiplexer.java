/*
Copyright (c) 2011, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package communication.readers;

import java.io.EOFException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import communication.channel.Record;
import communication.interfaces.RecordReader;

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

	public Record tryRead(int timeout, TimeUnit timeUnit) throws IOException {
		while(true) {
			try {
				return queue.poll(timeout, timeUnit);
			} catch (InterruptedException exception) {
				System.err.println("Unexpected thread interruption while waiting for a read");

				exception.printStackTrace();
			}
		}
	}

	public Record peek() {
		return queue.peek();
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
