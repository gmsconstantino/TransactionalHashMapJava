package fct.thesis.databaseNMSI;

import fct.thesis.database.*;
import pt.dct.util.P;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by Constantino Gomes on 21/05/15.
 */
public class ThreadCleanerNMSI<K,V> extends ThreadCleaner {

    private final static long WAITCLEANERTIME = Integer.getInteger("gc.cleaner", 100);
    private final static boolean VERBOSE_GC = Boolean.getBoolean("gc.verbose");
    public final Database db;

    public final static ConcurrentLinkedDeque<P<ObjectNMSIDb,Long>> set = new ConcurrentLinkedDeque<>();
    private final Storage storage;


    public ThreadCleanerNMSI(Database _db, Storage storage) {
        db = _db;
        this.storage = storage;
        setName("Thread-Cleaner");
    }

    public void run(){
        int tables = storage.getTablesNumber();
        while (!stop){

            long st=0, st2=0, en, en2, n=0, acum=0, max=0, min=Long.MAX_VALUE;

			sleep();

            if (VERBOSE_GC)
			    st = System.nanoTime();

            for (int i = 0; i < tables; i++) {
                Iterator<ObjectDb> it = storage.getIterator(i);
                while (it.hasNext()){
                    if (VERBOSE_GC)
					    st2 = System.nanoTime();

                    it.next().clean(-1);


                    if (VERBOSE_GC) {
                        n++;
                        en2 = System.nanoTime();
                        long t = en2-st2;


                        if (t > max)
                            max = t;
                        if (t < min)
                            min = t;
                        acum += t;
                    }
                }
            }

            if (VERBOSE_GC) {
                en = System.nanoTime();

                n = (n == 0 ? 1 : n);

                System.out.println(String.format("GC_INFO time=%dms objects=%d time/object=%dus max=%dus min=%dus",
                        ((en - st) / 1000000), n, ((acum / n) / 1000L), max/1000, min/1000));

            }

            
        }
    }

    private void sleep(){
        try {
            Thread.sleep(WAITCLEANERTIME);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
