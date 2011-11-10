/*
Copyright (c) 2010, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package communication;

import java.io.FileInputStream;

import java.io.FileNotFoundException;
import java.io.EOFException;
import java.io.IOException;

public class FileChannelElementReader implements ChannelElementReader {
	private ChannelElementInputStream channelElementInputStream;

	public FileChannelElementReader(String location) throws FileNotFoundException, IOException {
		channelElementInputStream = new ChannelElementInputStream(new FileInputStream(location));
	}

	public synchronized ChannelElement read() throws EOFException, IOException {
		return channelElementInputStream.readChannelElement();
	}

	public synchronized ChannelElement tryRead() throws EOFException, IOException {
		if(channelElementInputStream.available() > 0) {
			return read();
		}

		return null;
	}

	public void close() throws IOException {
		channelElementInputStream.close();
	}
}
