package mapreduce.programs;

import java.util.Set;

import java.util.Map;
import java.util.HashMap;

public abstract class Combiner<K,V> {
	private static final long serialVersionUID = 1L;

	private Map<K,V> currentTuples;

	public Combiner() {
		currentTuples = new HashMap<K,V>();
	}

	public void addTuple(K key, V newValue) {
		V updatedValue;

		V oldValue = currentTuples.get(key);

		if (oldValue != null) {
			updatedValue = combine(oldValue, newValue);
		}
		else {
			updatedValue = newValue;
		}

		currentTuples.put(key, updatedValue);
	}
	
	public V getValue(K key) {
		return currentTuples.get(key);
	}

	public Set<K> getKeys() {
		return currentTuples.keySet();
	}
	
	public Set<Map.Entry<K,V>> getTuples() {
		return currentTuples.entrySet();
	}

	public abstract V combine(V oldValue, V newValue);
}
