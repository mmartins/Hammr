package mapreduce.programs.counting;

import java.util.Comparator;

import mapreduce.communication.MRRecord;
import mapreduce.programs.Merger;

public class CountingMerger<K> extends Merger<K,Long> {
	private static final long serialVersionUID = 1L;

	public Comparator<MRRecord<K, Long>> getComparator() {
		return new MRRecordComparatorValue<K,Long>();
	}
}

class MRRecordComparator<K> implements Comparator<MRRecord<K,Long>> {
	public int compare(MRRecord<K, Long> first, MRRecord<K, Long> second) {
		return first.getValue().compareTo(second.getValue());
	}
}
