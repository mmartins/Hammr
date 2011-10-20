package mapreduce.programs.counting;

import java.util.Collections;
import java.util.Comparator;

import java.util.Map;
import java.util.Map.Entry;

import java.util.List;
import java.util.ArrayList;

import mapreduce.communication.MRRecord;

import mapreduce.programs.Reducer;

public class CountingReducer<K> extends Reducer<K,Long> {
	private static final long serialVersionUID = 1L;

	private CountingCombiner<K> combiner;

	public CountingReducer() {
		this.combiner = new CountingCombiner<K>();
	}

	public void reduce(K key, Long value) {
		combiner.addTuple(key, value);
	}

	public void flushReduce() {
		List<Map.Entry<K,Long>> tuples = new ArrayList<Map.Entry<K,Long>>(combiner.getTuples());

		Collections.sort(tuples, new CountingEntryComparator<K>());

		for (Map.Entry<K,Long> tuple: tuples) {
			K key = tuple.getKey();
			Long value = tuple.getValue();

			writeArbitraryChannel(new MRRecord<K,Long>(key, value));
		}
	}
}

class CountingEntryComparator<K> implements Comparator<Map.Entry<K,Long>> {
	public int compare(Entry<K, Long> first, Entry<K, Long> second) {
		return first.getValue().compareTo(second.getValue());
	}
}
