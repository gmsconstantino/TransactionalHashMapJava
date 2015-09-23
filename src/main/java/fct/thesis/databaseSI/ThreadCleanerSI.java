package fct.thesis.databaseSI;

import fct.thesis.database.*;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by Constantino Gomes on 21/05/15.
 */
public class ThreadCleanerSI<K,V> extends ThreadCleaner {


    private final static long WAITCLEANERTIME = Integer.getInteger("gc.cleaner", 100);
    private final static boolean VERBOSE_GC = Boolean.getBoolean("gc.verbose");

    public final static ConcurrentLinkedDeque<TransactionAbst> set = new ConcurrentLinkedDeque();

    private long min = -1;
    Storage storage;
    Database db;

    /*
     * Vejo a versao ate onde posso apagar pelas versoes que as transacoes activas esta a usar
     * obtenho o minimo dessas versoes e posso apagar as versoes abaixo dessa
     */
    public ThreadCleanerSI(Database db, Storage storage) {
        this.db = db;
        this.storage = storage;
        setName("Thread-Cleaner");
    }

    public long getMin() {
        long min = fct.thesis.databaseSI.Transaction.timestamp.get();
        Iterator<TransactionAbst> it = set.iterator();
        while (it.hasNext()){
            TransactionAbst t = it.next();

            while (t.id == -1) {}

            if (t.id < min){
                min = t.id;
            }
        }
        return min;
    }

    public void run(){
        int tables = storage.getTablesNumber();

        while (!stop){
			sleep();

            long st=0, st2=0, en, en2, n=0, acum=0, max=0, min=Long.MAX_VALUE;

            if (VERBOSE_GC)
			    st = System.nanoTime();

            long minVersion = getMin();
            if (minVersion == fct.thesis.databaseSI.Transaction.timestamp.get())
                continue;

            minVersion--;

            if (minVersion < 0)
                continue;

            for (int i = 0; i < tables; i++) {
                Iterator<ObjectDb> it = storage.getIterator(i);
                while (it.hasNext()){
                    if (VERBOSE_GC)
					    st2 = System.nanoTime();

                    it.next().clean(minVersion);

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
//            e.printStackTrace();
        }
    }
}
