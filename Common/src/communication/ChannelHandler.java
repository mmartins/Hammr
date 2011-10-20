package communication;

import java.io.EOFException;
import java.io.IOException;

import java.io.Serializable;

public abstract class ChannelHandler implements Serializable {
	private static final long serialVersionUID = 1L;

	private Type type;
	private Mode mode;
	private String name;

	private RecordReader recordReader;
	private RecordWriter recordWriter;

	public ChannelHandler(Type type, Mode mode, String name) {
		this.type = type;
		this.mode = mode;

		this.name = name;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public void setMode(Mode mode) {
		this.mode = mode;
	}

	public Mode getMode() {
		return mode;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public RecordReader getRecordReader() {
		return recordReader;
	}

	public void setRecordReader(RecordReader recordReader) {
		this.recordReader = recordReader;
	}

	public RecordWriter getRecordWriter() {
		return recordWriter;
	}

	public void setRecordWriter(RecordWriter recordWriter) {
		this.recordWriter = recordWriter;
	}

	public Record read() throws EOFException, IOException {
		return recordReader.read();
	}

	public boolean write(Record record) throws IOException {
		return recordWriter.write(record);
	}

	public boolean close() throws IOException {
		if (recordWriter != null) {
			return recordWriter.close();
		}

		return true;
	}

	public enum Type {
		SHM, TCP, FILE;
	}

	public enum Mode {
		INPUT, OUTPUT;
	}
}
