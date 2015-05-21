package fct.thesis.database;

import fct.thesis.database.Database;
import fct.thesis.database.ObjectDb;
import fct.thesis.database.Transaction;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by Constantino Gomes on 21/05/15.
 */
public class ThreadCleanerSI<K,V> extends Thread{


    private final static long WAITCLEANERTIME = 1000;

    public final PriorityBlockingQueue<Transaction> queue = Database.queue;

    private long min = -1;
    private Set<ObjectDb<K,V>> clean;

    public ThreadCleanerSI() {
        clean = new HashSet<>();
        setName("Thread-Cleaner");
    }

    public long getMin() {
        Transaction t = queue.peek();
        while (!queue.isEmpty() && t.id==(min+1)){
            min++;
            clean.addAll(t.getWriteSet());
            queue.poll();
        }
        return min;
    }

    public void run(){
        for (;;){
            if (queue.size()==0)
                sleep();

            long minTxId = getMin();

            Iterator<ObjectDb<K,V>> it = clean.iterator();
            while (it.hasNext()){
                ObjectDb<K,V> objectDb = it.next();
                objectDb.getObjectDb().clean(minTxId);
            }

            clean.clear();
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
