package fct.thesis.databaseNMSI;

import fct.thesis.database.*;
import fct.thesis.database.Transaction;
import fct.thesis.databaseNMSI.ObjectNMSIDb;
import fct.thesis.structures.MapEntry;
import pt.dct.util.P;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by Constantino Gomes on 21/05/15.
 */
public class ThreadCleanerNMSI<K,V> extends ThreadCleaner {

    private final static long WAITCLEANERTIME = 100;
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

            for (int i = 0; i < tables; i++) {
                Iterator<ObjectDb> it = storage.getObjectDbIterator(i);
                while (it.hasNext()){
                    it.next().clean(-1);
                }
            }

            sleep();
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
