/*

Copyright (c) 2011, Marcelo Martins

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.  Redistributions in binary
form must reproduce the above copyright notice, this list of conditions and the
following disclaimer in the documentation and/or other materials provided with
the distribution.  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.

*/

package utilities.dvfs;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class FreqStats
{
	private Map<Long, Long> stats;

	public FreqStats()
	{
		stats = new HashMap<Long, Long>();
	}

	public void addEntry(long freq, long duration)
	{
		stats.put(freq, duration);
	}

	public void removeEntry(long freq)
	{
		stats.remove(freq);
	}

	public long getDuration(long freq)
	{
		return stats.get(freq);
	}

	public Set<Long> getFreqs()
	{
		return stats.keySet();
	}

	public Collection<Long> getDurations()
	{
		return stats.values();
	}

	public long size()
	{
		return stats.size();
	}
}

