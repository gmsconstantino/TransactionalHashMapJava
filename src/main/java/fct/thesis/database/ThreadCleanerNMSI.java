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

    private final static long WAITCLEANERTIME = 2000;
    public final Database db;

    public ThreadCleanerNMSI(Database _db) {
        db = _db;
        setName("Thread-Cleaner");
    }

    public void run(){
        for (;;){

            sleep();

            Iterator<ObjectDb> it = db.getObjectDbIterator();
            while (it.hasNext()){
                it.next().clean(-1);
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
