package utilities;

import java.util.Random;

import java.util.Collection;

import java.util.List;
import java.util.ArrayList;

import communication.Record;
import communication.ChannelHandler;

import java.io.IOException;

public class RecordWriterShuffler {
	private List<ChannelHandler> channelHandlers;

	Random random;

	public RecordWriterShuffler(Collection<ChannelHandler> channelHandlers) {
		this.channelHandlers = new ArrayList<ChannelHandler>(channelHandlers);

		random = new Random();
	}

	public boolean writeArbitrary(Record record) throws IOException {
		if (channelHandlers.size() == 0) {
			return false;
		}

		int index = random.nextInt(channelHandlers.size());

		ChannelHandler channelHandler = channelHandlers.get(index);

		return channelHandler.write(record);
	}
}
