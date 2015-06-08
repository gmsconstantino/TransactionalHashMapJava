package fct.thesis.database;

import fct.thesis.database.Database;
import fct.thesis.database.ObjectDb;
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
public class ThreadCleanerNMSI<K,V> extends Thread{

    private final static long WAITCLEANERTIME = 100;
    public final Database db;

    public final static ConcurrentLinkedDeque<P<ObjectNMSIDb,Long>> set = new ConcurrentLinkedDeque<>();


    public ThreadCleanerNMSI(Database _db) {
        db = _db;
        setName("Thread-Cleaner");
    }

    public void run(){
        for (;;){

            sleep();

            Iterator<P<ObjectNMSIDb,Long>> it = set.iterator();
            while (it.hasNext()){
                P<ObjectNMSIDb,Long> pair = it.next();

                pair.f.clean(pair.s);
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
