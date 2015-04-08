package pt.dct;

import java.util.concurrent.atomic.AtomicInteger;

public class IdGen {
	public static AtomicInteger gen = new AtomicInteger(0);
	
	public static int newId() {
		return gen.incrementAndGet();
	}
}
