package communication;

import java.io.EOFException;
import java.io.IOException;

public interface RecordReader {
	public abstract Record read() throws EOFException, IOException;
	public abstract Record tryRead() throws EOFException, IOException;
	public abstract void close() throws IOException;
}
