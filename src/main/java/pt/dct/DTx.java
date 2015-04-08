package pt.dct;

import java.util.ArrayList;
import java.util.List;

import pt.dct.cli.AbortException;
import pt.dct.util.P;

public class DTx {
	
	public int rmin;
	public int rmax;
	public int wmin;
	public int wmax;
	
	public List<P<DObject,Object>> readSet = new ArrayList<P<DObject,Object>>();
	public List<DObject> undoLog = new ArrayList<DObject>();
	
	public int txId;
	
	
	public void start() {
		rmin = Integer.MAX_VALUE;
		rmax = Integer.MIN_VALUE;
		wmin = Integer.MAX_VALUE;
		wmax = Integer.MIN_VALUE;
		undoLog.clear();
		readSet.clear();
		
		txId = (int)Thread.currentThread().getId();
	}
	
	public boolean commit() {
		System.out.println("["+txId+"] COMMIT");
		
		boolean rollback = false;
		
		for (P<DObject,Object> rentry : readSet) {
			if ((rentry.f.writeLock.get() != txId && rentry.f.value != rentry.s) || 
					(rentry.f.writeLock.get() == txId && rentry.f.undoValue != rentry.s)) {
				rollback = true;
				break;
			}
		}
		
		for (DObject wentry : undoLog) {
			if (rollback) {
				wentry.value = wentry.undoValue;
			}
			wentry.writeLock.set(0);
		}
		
		
		
		if (rollback) {
			rollback();
		}
		
		return !rollback;
	}
	
	public <T> T read(DObject<T> obj) {
		if (obj.id < rmin) {
			rmin = obj.id;
		}
		
		if (obj.id > rmax) {
			rmax = obj.id;
		}
		
		int wlock1 = obj.writeLock.get();
		T val = obj.value;
		int wlock2 = obj.writeLock.get();
		
		if (wlock1 != wlock2) {
			abort();
		}
		else if (wlock1 == txId || wlock1 == 0) {
			readSet.add(new P<DObject,Object>(obj, obj.value));
			return val;
		}
		else {
			abort();
		}
		
		assert false;
		
		return null;
	}
	
	public <T> void write(DObject<T> obj, T newValue) {
		if (obj.id < wmin) {
			wmin = obj.id;
		}
		
		if (obj.id > wmax) {
			wmax = obj.id;
		}
		
		int wlock = obj.writeLock.get();
		
		boolean locked = false;
		
		if (wlock == 0) {
			locked = obj.writeLock.compareAndSet(wlock, txId);
			System.out.println("["+txId+"] LOCK1="+locked);
		}
		
		if (!locked) {
			if (obj.id < wmax) {
				abort();
			}
			else {
				while (true) {
					wlock = obj.writeLock.get();
					if (wlock == 0) {
						if (obj.writeLock.compareAndSet(wlock, txId)) {
							break;
						}
					}
				} 
				System.out.println("["+txId+"] LOCK2");
				undoLog.add(obj);
				obj.undoValue = obj.value;
				
				if (obj.wmin <= rmin || obj.wmax >= rmax) {
					abort();
				}
				
				obj.value = newValue;
				obj.rmin = rmin;
				obj.rmax = rmax;
				obj.wmin = wmin;
				obj.wmax = wmax;
			}
		}
		else {
			undoLog.add(obj);
			obj.undoValue = obj.value;
			obj.value = newValue;
			obj.rmin = rmin;
			obj.rmax = rmax;
			obj.wmin = wmin;
			obj.wmax = wmax;
		}
		
	}
	
	public void rollback() {
		System.out.println("["+txId+"] ROLLBACK");
	}
	
	private void abort() {
		for (DObject wentry : undoLog) {
			wentry.value = wentry.undoValue;
			wentry.writeLock.set(0);
		}
		throw AbortException.ABORT_EXCEPTION;
	}
	
	
	
	
	public static <T> T runTx(DTxBody<T> body) {
		DTx tx = new DTx();
		while(true) {
			tx.start();
			try {
				T val = body.body(tx);
				if (tx.commit()) {
					return val; 
				}
			}
			catch (AbortException e) {}
			
			tx.rollback();
		}
	}
	
}
