package mapreduce.programs;

import java.util.Comparator;

import java.util.Set;

import java.util.Map;
import java.util.HashMap;

import java.util.PriorityQueue;

import mapreduce.communication.MRRecord;

import appspecs.Node;

public abstract class Merger<K,V> extends Node {
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	public void run() {
		Set<String> inputs = getInputChannelNames();

		PriorityQueue<MRRecord<K,V>> records = new PriorityQueue<MRRecord<K,V>>(inputs.size(), getComparator());

		Map<MRRecord<K,V>,String> reverseMap = new HashMap<MRRecord<K,V>,String>();

		MRRecord<K,V> record;

		for (String input: inputs) {
			record = (MRRecord<K,V>) readChannel(input);

			if (record != null) {
				records.add(record);

				reverseMap.put(record, input);
			}
		}

		while (records.size() > 0) {
			record = records.poll();

			writeArbitraryChannel(record);

			String input = reverseMap.get(record);

			reverseMap.remove(record);

			record = (MRRecord<K,V>) readChannel(input);

			if(record != null) {
				records.add(record);

				reverseMap.put(record, input);
			}
		}

		closeOutputs();
	}

	public abstract Comparator<MRRecord<K,V>> getComparator();

	public class MRRecordComparatorObject<X extends Comparable<X>,Y> implements Comparator<MRRecord<X,Y>> {
		public int compare(MRRecord<X,Y> first, MRRecord<X,Y> second) {
			return first.getKey().compareTo(second.getKey());
		}
	}

	public class MRRecordComparatorValue<X,Y extends Comparable<Y>> implements Comparator<MRRecord<X,Y>> {
		public int compare(MRRecord<X,Y> first, MRRecord<X,Y> second) {
			return first.getValue().compareTo(second.getValue());
		}
	}
}
