package fct.thesis.database;

import fct.thesis.structures.MapEntry;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by gomes on 26/02/15.
 */
public class Database<K,V> {

    public static int numberTables;
    public ConcurrentHashMap[] tables;
    public final static ExecutorService asyncPool = Executors.newSingleThreadExecutor();

    public final static PriorityBlockingQueue<Transaction> queue = new PriorityBlockingQueue(1000000);
    private Thread cleaner;

    public Database(int ntables){
        numberTables = ntables;
        tables = new ConcurrentHashMap[ntables];
        for (int i = 0; i < ntables; i++) {
            tables[i] = new ConcurrentHashMap<K,ObjectDb<K,V>>(1000000,0.75f,256);
        }
    }

    protected ObjectDb<K,V> getKey(int table, K key){
        return (ObjectDb<K,V>) tables[table].get(key);
    }

    protected ObjectDb<K,V> putIfAbsent(int table, K key, ObjectDb<K,V> obj){
        return (ObjectDb<K,V>) tables[table].putIfAbsent(key, obj);
    }

    protected ObjectDb<K,V> removeKey(int table, K key){
        return (ObjectDb<K,V>) tables[table].remove(key);
    }

    public void cleanup(){
        asyncPool.shutdown();
    }

    public void startThreadCleaner(Thread _cleaner){
        cleaner = _cleaner;
        cleaner.setDaemon(true);
        cleaner.start();
    }

    /*
     * Public
     */

    public Transaction<K,V> newTransaction(TransactionFactory.type t){
        return TransactionFactory.createTransaction(t, this);
    }

    public int size(int table){
        return tables[table].size();
    }

    public Iterator getIterator(int table) {
        return new DbIterator(table);
    }
    
    protected Iterator<ObjectDb<K,V>> getObjectDbIterator(int table){
        return new ObjectDbIterator(table);
    }

    private class DbIterator implements Iterator {

        Set<Map.Entry<K, ObjectDb<K,V>>> set;
        Iterator<Map.Entry<K, ObjectDb<K,V>>> it;
        DbIterator(int table){
            set = tables[table].entrySet();
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

    private class ObjectDbIterator implements Iterator {

        Set<Map.Entry<K, ObjectDb<K,V>>> set;
        Iterator<Map.Entry<K, ObjectDb<K,V>>> it;
        ObjectDbIterator(int table){
            set = tables[table].entrySet();
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
                return obj.getValue();
            }
            return null;
        }

        @Override
        public void remove() {

        }
    }

}
