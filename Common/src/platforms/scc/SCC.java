/*
Copyright (c) 2011, Marcelo Martins

All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package platforms.scc;

public class SCC {

		public native int init(String[] argv);
		public native int terminate();
		public native int getTileSize();
		public native int getNodeRank();
		public native int getTileInfo();
		
		private int coreID;
		private int coordX;
		private int coordY;
		private int tileID;
		
		public static void main(String[] args)
		{
			SCC scc = new SCC();
			scc.init(args);
			scc.getTileInfo();
			
			System.out.println("Tile ID: " + scc.tileID);
			System.out.println("Core ID: " + scc.coreID);
			System.out.println("Tile coords: (" + scc.coordX + "," + scc.coordY + ")");
			System.out.println("Tile size: " + scc.getTileSize());
			scc.terminate();
		}

		static {
			System.load("scc");
		}
}
