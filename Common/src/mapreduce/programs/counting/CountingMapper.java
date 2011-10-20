package mapreduce.programs.counting;

import mapreduce.programs.Mapper;

public class CountingMapper<K> extends Mapper<K,Long> {
	private static final long serialVersionUID = 1L;

	public CountingMapper(int numberReducers) {
		super(numberReducers, new CountingCombiner<K>());
	}

	public Long map(K key) {
		return 1L;
	}
}
