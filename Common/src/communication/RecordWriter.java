package communication;

import java.io.IOException;

public interface RecordWriter {
	public abstract boolean write(Record channelElement) throws IOException;
	public abstract boolean flush() throws IOException;
	public abstract boolean close() throws IOException;
}
