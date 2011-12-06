/*
Copyright (c) 2010, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package mapreduce.programs;

import java.util.Map;
import java.util.Set;

import mapreduce.communication.MRRecord;
import appspecs.Node;

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

		shutdown();
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
