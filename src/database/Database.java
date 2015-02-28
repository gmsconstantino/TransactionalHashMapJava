package database;

import structures.MapEntry;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 26/02/15.
 */
public class Database<K,V> {

    public final long TIMEOUT = 2;
    public final TimeUnit TIMEOUT_UNIT = TimeUnit.MICROSECONDS;

    ConcurrentHashMap<K, ObjectDb<V>> concurrentHashMap;

    public Database(){
        concurrentHashMap = new ConcurrentHashMap<K, ObjectDb<V>>();
    }

    public V get(Transaction t, K key) throws TransactionTimeoutException {

        ObjectDb<V> obj = t.get_from_cache(key);
        if (obj != null){
            return obj.getValue();
        }

        // Passa a ser um objecto do tipo ObjectDbImpl
        obj = concurrentHashMap.get(key);

        if (obj == null)
            return null;

        if(obj.try_lock_read_for(TIMEOUT, TIMEOUT_UNIT)){
            t.add_Read_ObjectDb(key, obj);
            return obj.getValue();
        } else {
            t.abort();
            throw new TransactionTimeoutException("Transaction " + t.getId() +": Thread "+Thread.currentThread().getName()+" - get key:"+key);
        }
    }

    public V get_to_update(Transaction t, K key) throws TransactionTimeoutException {

        ObjectDb<V> obj = t.get_from_cache(key);
        if (obj != null){
            return obj.getValue();
        }

        // Passa a ser um objecto do tipo ObjectDbImpl
        obj = concurrentHashMap.get(key);

        if (obj == null)
            return null;

        if(obj.try_lock_write_for(TIMEOUT, TIMEOUT_UNIT)){
            t.add_Write_ObjectDb(key, obj);
            return obj.getValue();
        } else {
            t.abort();
            throw new TransactionTimeoutException("Transaction " + t.getId() +": Thread "+Thread.currentThread().getName()+" - get key:"+key);
        }
    }

    public void put(Transaction t, K key, V value) throws TransactionTimeoutException {

        // Se esta na cache é pq ja tenho o write lock do objecto
        ObjectDb<V> obj = t.get_from_cache(key);
        if (obj != null){
            obj.setValue(value);
            return;
        }

        // Search
        obj = concurrentHashMap.get(key); // Passa a ser um objecto do tipo ObjectDbImpl
        boolean add_ok = false;
        if(obj == null){
            obj = new ObjectDbImpl<V>(value); // A thread fica com o write lock

            ObjectDb<V> map_obj = concurrentHashMap.putIfAbsent(key, obj);
            obj = map_obj!=null? map_obj : obj;

            t.add_Write_ObjectDb(key, obj);
            t.put_to_cache(key, new CacheObjectDb<V>(value));
            return;
        }

        if(obj.try_lock_write_for(TIMEOUT,TIMEOUT_UNIT)){
            t.add_Write_ObjectDb(key, obj);
            t.put_to_cache(key, new CacheObjectDb<V>(value));
//            obj.value = value;
        } else {
            t.abort();
            throw new TransactionTimeoutException("Transaction " + t.getId() +": Thread "+Thread.currentThread().getName()+" - put key:"+key+" value:"+value);
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

    public int size(){
        return concurrentHashMap.size();
    }

    public Iterator getIterator() {
        return new DbIterator();
    }

    private class DbIterator implements Iterator {

        Set<Map.Entry<K, ObjectDb<V>>> set;
        Iterator<Map.Entry<K, ObjectDb<V>>> it;
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
                Map.Entry<K, ObjectDb<V>> obj = it.next();
                ObjectDb<V> objectDb = obj.getValue();
                return new MapEntry<K,V>(obj.getKey(),objectDb.getValue());
            }
            return null;
        }

        @Override
        public void remove() {

        }
    }

}
