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
