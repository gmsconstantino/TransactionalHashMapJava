package fct.thesis.database;

import fct.thesis.databaseNMSI.ObjectBlotterDb;
import fct.thesis.structures.MapEntry;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by gomes on 26/02/15.
 */
public class Database<K,V> {

    private final static long WAITCLEANERTIME = 1000;

    public final static ExecutorService asyncPool = Executors.newFixedThreadPool(5);

    public final static PriorityBlockingQueue<Transaction> queue = new PriorityBlockingQueue(1000000);
    private final ThreadCleaner cleaner = new ThreadCleaner();

    public ConcurrentHashMap<K, ObjectDb<K,V>> concurrentHashMap;

    public Database(){
        concurrentHashMap = new ConcurrentHashMap<K, ObjectDb<K,V>>(1000000,0.75f,64);
    }

    protected ObjectDb<K,V> getKey(K key){
        return concurrentHashMap.get(key);
    }

    protected ObjectDb<K,V> putIfAbsent(K key, ObjectDb<K,V> obj){
        return concurrentHashMap.putIfAbsent(key, obj);
    }

    protected ObjectDb<K,V> removeKey(K key){
        return concurrentHashMap.remove(key);
    }

    public void cleanup(){
        asyncPool.shutdown();
    }

    public void startThreadCleaner(){
        cleaner.setDaemon(true);
        cleaner.start();
    }

    /*
     * Public
     */

    public Transaction<K,V> newTransaction(TransactionFactory.type t){
        return TransactionFactory.createTransaction(t, this);
    }

    public int size(){
        return concurrentHashMap.size();
    }

    public Iterator getIterator() {
        return new DbIterator();
    }

    private class DbIterator implements Iterator {

        Set<Map.Entry<K, ObjectDb<K,V>>> set;
        Iterator<Map.Entry<K, ObjectDb<K,V>>> it;
        DbIterator(){
            set = concurrentHashMap.entrySet();
            it = set.iterator();
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public Object next() {

            if(this.hasNext()){
                Map.Entry<K, ObjectDb<K,V>> obj = it.next();
                ObjectDb<K,V> objectDb = obj.getValue();
                return new MapEntry<K,V>(obj.getKey(),objectDb.getValue());
            }
            return null;
        }

        @Override
        public void remove() {

        }
    }

    private class ThreadCleaner extends Thread {

        private long min = -1;
        private Set<ObjectDb<K,V>> clean;

        public long getMin() {
            Transaction t = queue.peek();
            while (!queue.isEmpty() && t.id==(min+1)){
                min++;
                clean.addAll(t.getWriteSet());
                queue.poll();
            }
            return min;
        }

        public ThreadCleaner() {
            clean = new HashSet<>();
        }

        public void run(){
            for (;;){
                if (queue.size()==0)
                    sleep();

                long minId = getMin();

                Iterator<ObjectDb<K,V>> it = clean.iterator();
                while (it.hasNext()){
                    ObjectDb<K,V> objectDb = it.next();
                    objectDb.clean(minId);
                }

                clean.clear();

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
}
