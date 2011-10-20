package programs;

import appspecs.Node;

import communication.Record;

public class ReaderArbitraryWriterAll extends Node {
	private static final long serialVersionUID = 1L;

	public void run() {
		Record record;

		while (true) {
			record = readArbitraryChannel();

			if(record == null) {
				break;
			}

			writeAllChannels(record);
		}

		closeOutputs();
	}
}
