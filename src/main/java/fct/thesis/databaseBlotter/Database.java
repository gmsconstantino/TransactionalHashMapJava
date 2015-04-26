package fct.thesis.databaseBlotter;

import fct.thesis.database.ObjectDb;
import fct.thesis.database.Transaction;
import fct.thesis.database.TransactionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by gomes on 26/02/15.
 */
public class Database<K,V> extends fct.thesis.database.Database{

    private static final Logger logger = LoggerFactory.getLogger(Database.class);

    public Object monitor;
    public LinkedBlockingDeque<Long> queue;
    public ThreadCleaner cleanerThr;

//    private final long waitCleanerTime = 1000;

    public Database(Properties properties){
        this();
    }

    public Database(){
        super();
        queue = new LinkedBlockingDeque<Long>();
        monitor = new Object();
        cleanerThr = new ThreadCleaner(monitor);
        cleanerThr.setDaemon(true);
        cleanerThr.start();
    }

    public void addTransactiontoClean(long tid){
        queue.offer(tid);

        if (queue.size() > 10000)
            synchronized (monitor) {
                monitor.notify();
            }
    }

    public void cleanup(){
        fct.thesis.databaseBlotter.Transaction.service.shutdown();
    }

    private class ThreadCleaner extends Thread {

        private final Object monitor;
        ArrayList<Long> cleanTid = new ArrayList<Long>();

        public ThreadCleaner(Object mon) {
            this.monitor = mon;
        }

        public void run(){
            synchronized (monitor){
                try {
                    monitor.wait();
                } catch (InterruptedException e) {
                    System.err.println("Exception Thread Cleaner.");
                    return;
                }
            }
            while (true){
                logger.debug("Running Cleaner.");
                for (int i=0; i<8000; i++){
                    try {
                        cleanTid.add(queue.takeFirst());
                    } catch (InterruptedException e) {
//                        e.printStackTrace();
                    }
                }

//                Iterator<ObjectDb<K,V>> it = concurrentHashMap.values().iterator();
//                while (it.hasNext()){
                for (ObjectDb<K,V> obj : db) {
                    ObjectBlotterDb<K,V> objectDb = (ObjectBlotterDb) obj;
                    for (Long tid : cleanTid)
                        objectDb.removeSnapshot(tid);
                }

                cleanTid.clear();

                logger.debug("Done Cleaning.");
                synchronized (monitor) {
                    try {
                        monitor.wait();
                    } catch (InterruptedException e) {
                        System.err.println("Exception Thread Cleaner.");
//                    e.printStackTrace();
                        return;
                    }
                }
            }
        }

    }

}
