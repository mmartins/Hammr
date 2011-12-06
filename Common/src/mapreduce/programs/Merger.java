/*
Copyright (c) 2010, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package mapreduce.programs;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

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

		shutdown();
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
