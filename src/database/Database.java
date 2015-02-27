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

    ConcurrentHashMap<K,ObjectDb<K,V>> concurrentHashMap;
    Lock safe_put;

    public Database(){
        concurrentHashMap = new ConcurrentHashMap<K, ObjectDb<K, V>>();
        safe_put = new ReentrantLock();
    }

    public V get(Transaction t, K key) throws TransactionAbortException {
        ObjectDb<K,V> obj = concurrentHashMap.get(key);

        if (obj == null)
            return null;

        if(obj.rwlock.try_lock_read_for(TIMEOUT, TIMEOUT_UNIT)){
            // add to list lock transactions
            t.addLock(obj.rwlock.getObjectLock());

            return obj.value;
        } else {
            t.abort();
            throw new TransactionAbortException("Thread "+Thread.currentThread().getName()+" - get key:"+key);
        }
    }

    public void put(Transaction t, K key, V value) throws TransactionAbortException{
        // Search
        ObjectDb<K,V> obj = concurrentHashMap.get(key);

        if(obj == null){
            safe_put.lock(); //Sendo a hash concorrente necessito disto?
            obj = concurrentHashMap.get(key);
            if(obj == null) {
                obj = new ObjectDb<K, V>(key);
                concurrentHashMap.put(key, obj);
            }
            safe_put.unlock();
        }

        //falta verificar se a transacao ten o lock
        if(obj.rwlock.try_lock_write_for(TIMEOUT,TIMEOUT_UNIT)){

        } else {
            t.abort();
            throw new TransactionAbortException("Thread "+Thread.currentThread().getName()+" - put key:"+key+" value:"+value);
        }


        obj.value = value;

        // add to list lock transactions
        t.addLock(obj.rwlock.getObjectLock());

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
