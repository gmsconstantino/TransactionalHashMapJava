package fct.thesis.databaseNMSI;

import fct.thesis.database.*;
import pt.dct.util.P;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

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
                Iterator<ObjectDb> it = storage.getIterator(i);
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
