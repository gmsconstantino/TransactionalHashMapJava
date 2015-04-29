package pt.dct.cli;

public abstract class TxCmd {
	
	protected ClientThread ct;
	
	public void setClientThread(ClientThread ct) {
		this.ct = ct;
	}
	
	public abstract void execute();

	public static class BeginCmd extends TxCmd {

		@Override
		public void execute() {
			ct.beginCmd();
		}
		
	}
	
	public static class CommitCmd extends TxCmd {

		
		@Override
		public void execute() {
			ct.commitCmd();
		}
		
	}
	
	public static class ReadCmd extends TxCmd {

		protected String key;
		
		public ReadCmd(String[] params) {
			key = params[1];
		}
		
		@Override
		public void execute() {
			ct.readCmd(key);
		}
		
	}
	
	public static class WriteCmd extends TxCmd {

		protected String key;
		protected int val;
		
		public WriteCmd(String[] params) {
			key = params[1];
			val = Integer.parseInt(params[2]);
		}
		
		@Override
		public void execute() {
			ct.writeCmd(key, val);
		}
		
	}
}
