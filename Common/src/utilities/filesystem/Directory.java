/*
Copyright (c) 2011, Hammurabi Mendes
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package utilities.filesystem;

import java.io.Serializable;

public class Directory implements Serializable {
	private static final long serialVersionUID = 1L;

	private String path;
	private Protocol protocol;

	public Directory(String location) {
		this(location, Protocol.POSIX_COMPATIBLE);
	}

	public Directory(String path, Protocol protocol) {
		boolean slashPresent = path.endsWith("/");

		this.path = (slashPresent ? path : path + "/");
		this.protocol = protocol;
	}

	public String getPath() {
		return path;
	}

	public Protocol getProtocol() {
		return protocol;
	}

	public boolean equals(Directory other) {
		return (this.getPath() == other.getPath() && this.getProtocol() == other.getProtocol());
	}

	public int hashCode() {
		return path.hashCode() + protocol.hashCode();
	}

	public String toString() {
		return protocol + path;
	}
}
