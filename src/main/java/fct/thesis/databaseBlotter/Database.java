package fct.thesis.databaseBlotter;

import fct.thesis.database.ObjectDb;
import fct.thesis.database.Transaction;
import fct.thesis.database.TransactionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by gomes on 26/02/15.
 */
public class Database<K,V> extends fct.thesis.database.Database{

//    public final static ExecutorService service = Executors.newFixedThreadPool(10);
//
//    public Object monitor;
//    public LinkedBlockingDeque<Long> queue;
//    public ThreadCleaner cleanerThr;

//    private final long waitCleanerTime = 1000;

    public Database(Properties properties){
        this();
    }

    public Database(){
        super();
//        queue = new LinkedBlockingDeque<Long>();
//        monitor = new Object();
//        cleanerThr = new ThreadCleaner(monitor);
//        cleanerThr.setDaemon(true);
//        cleanerThr.start();
    }

    public void addTransactiontoClean(long tid){
//        queue.offer(tid);
//
//        if (queue.size() > 10000)
//            synchronized (monitor) {
//                monitor.notify();
//            }
    }

    public void cleanup(){
//        service.shutdown();
    }

//    private class ThreadCleaner extends Thread {
//
//        private final Object monitor;
//        ArrayList<Long> cleanTid = new ArrayList<Long>();
//
//        public ThreadCleaner(Object mon) {
//            this.monitor = mon;
//        }
//
//        public void run(){
//            setName("Thread-Cleanner");
//            synchronized (monitor){
//                try {
//                    monitor.wait();
//                } catch (InterruptedException e) {
//                    System.err.println("Exception Thread Cleaner.");
//                    return;
//                }
//            }
//            while (true){
//                for (int i=0; i<8000; i++){
//                    try {
//                        cleanTid.add(queue.takeFirst());
//                    } catch (InterruptedException e) {
////                        e.printStackTrace();
//                    }
//                }
//
//                Iterator<ObjectDb<K,V>> it = concurrentHashMap.values().iterator();
//                while (it.hasNext()){
//                    ObjectBlotterDb<K,V> objectDb = (ObjectBlotterDb) it.next();
//                    for (Long tid : cleanTid)
//                        objectDb.removeSnapshot(tid);
//                }
//
//                cleanTid.clear();
//
//                synchronized (monitor) {
//                    try {
//                        monitor.wait();
//                    } catch (InterruptedException e) {
//                        System.err.println("Exception Thread Cleaner.");
////                    e.printStackTrace();
//                        return;
//                    }
//                }
//            }
//        }
//
//    }

}
