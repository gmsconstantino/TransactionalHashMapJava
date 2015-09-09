package fct.thesis.databaseSI;

import fct.thesis.database.*;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by Constantino Gomes on 21/05/15.
 */
public class ThreadCleanerSI<K,V> extends ThreadCleaner {


    private final static long WAITCLEANERTIME = 100;

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

            long minVersion = getMin();
            if (minVersion == fct.thesis.databaseSI.Transaction.timestamp.get())
                continue;

            minVersion--;

            if (minVersion < 0)
                continue;

            for (int i = 0; i < tables; i++) {
                Iterator<ObjectDb> it = storage.getIterator(i);
                while (it.hasNext()){
                    it.next().clean(minVersion);
                }
            }

            sleep();
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
