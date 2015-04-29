package pt.dct.test;

import pt.dct.DObject;
import pt.dct.DTx;
import pt.dct.DTxBody;

public class Counter {
	
	private DObject<Integer> counter = new DObject<Integer>(0);
	
	public void inc() {
		DTx.runTx(new DTxBody<Void>() {
			@Override
			public Void body(DTx tx) {
				Integer i = tx.read(counter);
				System.out.println("["+tx.txId+"] READ="+i);
				tx.write(counter, i+1);
				System.out.println("["+tx.txId+"] WROTE="+(i+1));
				return null;
			}
		});
	}
	
	public int get() {
		return DTx.runTx(new DTxBody<Integer>() {
			@Override
			public Integer body(DTx tx) {
				return tx.read(counter);
			}
		});
	}

}
