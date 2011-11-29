/*
Copyright (c) 2010, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package communication.writers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import communication.channel.Record;
import communication.interfaces.RecordWriter;
import communication.streams.RecordOutputStream;

public class TCPRecordWriter implements RecordWriter {
	private String name;
	private RecordOutputStream recordOutputStream;

	public TCPRecordWriter(String name, InetSocketAddress socketAddress) throws IOException {
		this.name = name;

		Socket socket = new Socket(socketAddress.getAddress(), socketAddress.getPort());

		this.recordOutputStream = new RecordOutputStream(socket.getOutputStream());

		recordOutputStream.writeObject(name);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean write(Record record) throws IOException {
		recordOutputStream.writeRecord(record);

		return true;
	}

	public boolean flush() throws IOException {
		recordOutputStream.flush();
		recordOutputStream.reset();

		return true;
	}

	public boolean close() throws IOException {
		recordOutputStream.flush();

		recordOutputStream.close();

		return true;
	}
}
