package pt.dct.test;

public class Test {
	
	
	public static class Worker implements Runnable {
		
		Counter shared;
		int n;
		
		public Worker(Counter c, int n) {
			this.shared = c;
			this.n = n;
		}
		
		public void run() {
			while(n > 0) {
				shared.inc();
				n--;
			}
		}
		
	}
	
	public static void main(String[] args) throws InterruptedException {
		
		Counter c = new Counter();
		
		int t = Integer.parseInt(args[0]);
		int n = Integer.parseInt(args[1]);
		
		Thread[] workers = new Thread[t];
		
		for (int i=0; i < t; i++) {
			workers[i] = new Thread(new Worker(c, n));
		}
		
		for (int i=0; i < t; i++) {
			workers[i].start();
		}
		
		for (int i=0; i < t; i++) {
			workers[i].join();
		}
		
		
		System.out.println(c.get());
		
	}
	
	
}
