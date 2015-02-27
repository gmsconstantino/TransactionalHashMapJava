package database;

import structures.MapEntry;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by gomes on 26/02/15.
 */
public class Database<K,V> {

    public final long TIMEOUT = 2000;
    public final TimeUnit TIMEOUT_UNIT = TimeUnit.MICROSECONDS;

    public static long commit_count = 0;

    ConcurrentHashMap<K,ObjectDb<K,V>> concurrentHashMap;
    Lock safe_put;

    public Database(){
        concurrentHashMap = new ConcurrentHashMap<K, ObjectDb<K, V>>();
        safe_put = new ReentrantLock();
    }

    public V get(Transaction t, K key) throws TransactionTimeoutException {
        ObjectDb<K,V> obj = concurrentHashMap.get(key);

        if (obj == null)
            return null;

        if(obj.try_lock_read_for(t,TIMEOUT, TIMEOUT_UNIT)){
            return obj.value;
        } else {
            t.abort();
            throw new TransactionTimeoutException("Thread "+Thread.currentThread().getName()+" - get key:"+key);
        }
    }

    public void put(Transaction t, K key, V value) throws TransactionTimeoutException {
        // Search
        ObjectDb<K,V> obj = concurrentHashMap.get(key);
        boolean add_ok = false;
        if(obj == null){
            safe_put.lock(); //Sendo a hash concorrente necessito disto?
            obj = concurrentHashMap.get(key);
            if(obj == null) {
                obj = new ObjectDb<K, V>(t,key, value); // A thread fica com o write lock
                concurrentHashMap.put(key, obj);
                add_ok = true;
            }
            safe_put.unlock();
            if(add_ok)
                return;
        }


        if(obj.try_lock_write_for(t,TIMEOUT,TIMEOUT_UNIT)){
            obj.value = value;
        } else {
            t.abort();
            throw new TransactionTimeoutException("Thread "+Thread.currentThread().getName()+" - put key:"+key+" value:"+value);
        }

    }

    public Transaction txn_begin(){
        return new Transaction();
    }

    public boolean txn_commit(Transaction txn){
        txn.commit();

        return true;
    }

    public void txn_abort(Transaction txn){
        txn.abort();
    }


    public Iterator getIterator() {
        return new DbIterator();
    }

    private class DbIterator implements Iterator {

        Set<Map.Entry<K,ObjectDb<K,V>>> set;
        Iterator<Map.Entry<K,ObjectDb<K,V>>> it;
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
                ObjectDb<K,V> objectDb = it.next().getValue();
                return new MapEntry<K,V>(objectDb.key,objectDb.value);
            }
            return null;
        }

        @Override
        public void remove() {

        }
    }

}
