package fct.thesis.database;

import fct.thesis.database.Database;
import fct.thesis.database.ObjectDb;
import fct.thesis.database.Transaction;
import fct.thesis.structures.MapEntry;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by Constantino Gomes on 21/05/15.
 */
public class ThreadCleanerNMSI<K,V> extends Thread{

    private final static long WAITCLEANERTIME = 100;
    public final PriorityBlockingQueue<Transaction> queue = Database.queue;

    public final Database db;
    private long min = -1;

    public ThreadCleanerNMSI(Database _db) {
        db = _db;
        setName("Thread-Cleaner");
    }

    public long getMin() {
        Transaction t = queue.peek();
        while (!queue.isEmpty() && t.id==(min+1)){
            min++;
            queue.poll();
            t = queue.peek();
        }
        return min;
    }


    public void run(){
        for (;;){
            if (queue.size()==0) {
                sleep();
                continue;
            }

            long minTxId = getMin();

            Iterator<ObjectDb> it = db.getObjectDbIterator();
            while (it.hasNext()){
                it.next().clean(minTxId);
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
