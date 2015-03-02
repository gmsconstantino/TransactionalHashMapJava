package pt.dct.cli;

public class ClientThread extends Thread {
	
	protected volatile boolean active;
	protected volatile TxCmd nextCmd = null;
	protected volatile Object waitObj = new Object();
	
	protected final TxStorage storage;
	
	protected Tx currTx;
	
	public ClientThread(String key, TxStorage storage) {
		super(key);
		this.active = true;
		this.storage = storage;
	}
	
	@Override
	public void run() {
		while(active) {
			getNextCmd();
			if (nextCmd == null)
				continue;
			executeCmd();
			finishCmd();
		}
	}

	protected void finishCmd() {
		synchronized (waitObj) {
			nextCmd = null;
			waitObj.notify();
		}
	}

	protected void executeCmd() {
		nextCmd.execute();
		
	}

	protected void getNextCmd() {
		synchronized (this) {
			while(nextCmd == null && active) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public void submitCmd(TxCmd cmd) {
		synchronized (this) {
			nextCmd = cmd;
			nextCmd.setClientThread(this);
			this.notify();
		}
		synchronized (waitObj) {
			try {
				waitObj.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}

	public void finish() {
		this.active = false;
		
		synchronized (this) {
			this.notify();
		}
	}
	
	
	public void beginCmd() {
		if (currTx != null) {
			System.out.println("error: transaction already initiated");
			return;
		}
		
		currTx = storage.createTransaction();
		System.out.println("# ["+getName()+"] transaction started");
	}
	
	public void writeCmd(String key, int val) {
        try {
            if (currTx == null) {
                currTx = storage.createTransaction();
                currTx.write(key, val);
                currTx.commit();
                currTx = null;
            } else {
                currTx.write(key, val);
            }
        } catch (AbortException e){
            if (e.getMessage() != null)
                System.out.println("error: abort exception - "+e.getMessage());
            else
                System.out.println("error: abort exception");
        }
	}
	
	public void readCmd(String key) {
		int val;
		try {
			if (currTx == null) {
				currTx = storage.createTransaction();
				val = currTx.read(key);
				currTx.commit();
				currTx = null;
			}
			else {
				val = currTx.read(key);
			}
			
			System.out.println("# "+val);
		}
		catch (KeyNotFoundException e) {
			System.out.println("error: key not found");
		}
        catch (AbortException e){
            if (e.getMessage() != null)
                System.out.println("error: abort exception - "+e.getMessage());
            else
                System.out.println("error: abort exception");
        }
	}
	
	public void commitCmd() {
		if (currTx == null) {
			System.out.println("error: no transaction initiated");
			return;
		}
		if (currTx.commit()) {
			System.out.println("# ["+getName()+"] transaction committed");
		}
		else {
			System.out.println("# ["+getName()+"] transaction aborted");
		}
		currTx = null;
	}


}
