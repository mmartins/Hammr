package communication;

import java.net.Socket;
import java.net.InetSocketAddress;

import java.io.IOException;

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
