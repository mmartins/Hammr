package communication;

import java.io.IOException;

public class SHMRecordWriter implements RecordWriter {
	private String name;
	private SHMRecordMultiplexer recordMultiplexer;

	public SHMRecordWriter(String name, SHMRecordMultiplexer shmRecordMultiplexer) {
		this.name = name;

		this.recordMultiplexer = shmRecordMultiplexer;
	}

	public boolean write(Record record) throws IOException {
		recordMultiplexer.write(name, record);

		return true;
	}

	public boolean flush() throws IOException {
		return true;
	}

	public boolean close() throws IOException {
		recordMultiplexer.close(name);

		return true;
	}
}
