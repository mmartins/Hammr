package mapreduce.communication;

import communication.Record;

public class MRRecord<K,V> extends Record {
	private static final long serialVersionUID = 1L;

	private V value;

	public MRRecord(K key, V value) {
		super(key, null);

		this.value = value;
	}

	@SuppressWarnings("unchecked")
	public K getKey() {
		return (K) super.getObject();
	}

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}
}
