package mapreduce.programs;

import java.util.Set;
import java.util.Map;

import appspecs.Node;

import mapreduce.communication.MRRecord;

public abstract class Mapper<K,V> extends Node {
	private static final long serialVersionUID = 1L;

	protected int numberReducers;

	protected Combiner<K,V> combiner;

	public Mapper(int numberReducers) {
		this(numberReducers, null);
	}

	public Mapper(int numberReducers, Combiner<K,V> combiner) {
		this.numberReducers = numberReducers;

		this.combiner = combiner;
	}

	public int getNumberReducers() {
		return numberReducers;
	}

	public void setNumberReducers(int numberReducers) {
		this.numberReducers = numberReducers;
	}

	public Combiner<K,V> getCombiner() {
		return combiner;
	}

	public void setCombiner(Combiner<K,V> combiner) {
		this.combiner = combiner;
	}

	@SuppressWarnings("unchecked")
	public void run() {
		MRRecord<K,V> record;

		while(true) {
			record = (MRRecord<K,V>) readArbitraryChannel();

			if(record == null) {
				break;
			}

			K key = record.getKey();

			V value = map(key);

			if (combiner == null) {
				record.setValue(value);

				String destination = getDestination(key);

				writeChannel(record, destination);
			}
			else {
				combiner.addTuple(key, value);
			}
		}

		flushMap();

		closeOutputs();
	}

	protected String getDestination(K key) {
		return "reducer-" + Math.abs(key.hashCode() % numberReducers);
	}

	protected abstract V map(K key);

	protected void flushMap() {
		if (combiner != null) {
			Set<Map.Entry<K,V>> tuples = combiner.getTuples();

			for (Map.Entry<K,V> tuple: tuples) {
				K key = tuple.getKey();
				V value = tuple.getValue();

				String destination = getDestination(key);

				writeChannel(new MRRecord<K,V>(key, value), destination);
			}
		}
	}
}
