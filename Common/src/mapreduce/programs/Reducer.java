package mapreduce.programs;

import appspecs.Node;

import mapreduce.communication.MRRecord;

public abstract class Reducer<K,V> extends Node {
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	public void run() {
		MRRecord<K,V> record;

		while (true) {
			record = (MRRecord<K,V>) readArbitraryChannel();

			if (record == null) {
				break;
			}

			reduce(record.getKey(), record.getValue());
		}

		flushReduce();

		closeOutputs();
	}

	protected abstract void reduce(K key, V value);
	protected abstract void flushReduce();
}
