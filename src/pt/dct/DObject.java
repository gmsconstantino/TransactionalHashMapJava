package pt.dct;

import java.util.concurrent.atomic.AtomicInteger;

public class DObject<T> {
	
	public int id = IdGen.newId();
	public T value;
	
	public AtomicInteger readLock = new AtomicInteger(0); // 0 - unlocked, > 0 - locked 
	public AtomicInteger writeLock = new AtomicInteger(0); // 0 - unlocked, > 0 - locked
	public int rmin = Integer.MAX_VALUE;
	public int rmax = Integer.MIN_VALUE;
	public int wmin = Integer.MAX_VALUE;
	public int wmax = Integer.MIN_VALUE;
	public T undoValue;
	
	public DObject(T value) {
		this.value = value;
	}
	
	public DObject() {
		this((T)null);
	}
	
	
}
