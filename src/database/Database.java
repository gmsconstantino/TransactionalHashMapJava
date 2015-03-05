package database;

import structures.MapEntry;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by gomes on 26/02/15.
 */
public class Database<K,V> {

    public static AtomicLong timestamp = new AtomicLong(0);
    ConcurrentHashMap<K, ObjectDb<V>> concurrentHashMap;

    public Database(){
        concurrentHashMap = new ConcurrentHashMap<K, ObjectDb<V>>();
    }

    protected ObjectDb<V> getKey(K key){
        return concurrentHashMap.get(key);
    }

    protected ObjectDb<V> putIfAbsent(K key, ObjectDb<V> obj){
        return concurrentHashMap.putIfAbsent(key, obj);
    }

    protected ObjectDb<V> removeKey(K key){
        return concurrentHashMap.remove(key);
    }

//    public V get_to_update(Transaction t, K key) throws TransactionTimeoutException {
//        if (!t.isActive)
//            return null;
//
//        ObjectDb<V> obj = t.getObjectFromWriteBuffer(key);
//        if (obj != null){
//            return obj.getValue();
//        }
//
//        // Passa a ser um objecto do tipo ObjectDbImpl
//        obj = concurrentHashMap.get(key);
//
//        if (obj == null)
//            return null;
//
//        if(obj.try_lock_write_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){
//            t.addObjectDbToWriteBuffer(key, new CacheObjectDb(obj.getValue(), obj));
//            return obj.getValue();
//        } else {
//            txn_abort(t);
//            throw new TransactionTimeoutException("Transaction " + t.getId() +": Thread "+Thread.currentThread().getName()+" - get key:"+key);
//        }
//    }
//
//    public void put(Transaction t, K key, V value) throws TransactionTimeoutException {
//        if (!t.isActive)
//            return;
//
//        // Se esta na cache é pq ja tenho o write lock do objecto
//        ObjectDb<V> obj = t.getObjectFromWriteBuffer(key);
//        if (obj != null){
//            obj.setValue(value);
//            return;
//        }
//
//        // Search
//        obj = concurrentHashMap.get(key); // Passa a ser um objecto do tipo ObjectDbImpl
//        boolean add_ok = false;
//        if(obj == null){
//            obj = new ObjectDbImpl<V>(value); // A thread fica com o write lock
//
//            ObjectDb<V> map_obj = concurrentHashMap.putIfAbsent(key, obj);
//            obj = map_obj!=null? map_obj : obj;
//
//            t.addObjectDbToWriteBuffer(key, obj);
//
//            return;
//        }
//
//        if(obj.try_lock_write_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){
//            t.addObjectDbToWriteBuffer(key, new CacheObjectDb<V>(obj.getValue(), obj));
//            obj.setValue(value);
//        } else {
//            txn_abort(t);
//            throw new TransactionTimeoutException("Transaction " + t.getId() +": Thread "+Thread.currentThread().getName()+" - put key:"+key+" value:"+value);
//        }
//    }

    public Transaction<K,V> newTransaction(TransactionFactory.type t){
        return TransactionFactory.getFactory(t,this);
    }

//    public boolean txn_commit(Transaction txn){
//        if (!txn.isActive)
//            return txn.success;
//
//        txn.commit_id = Transaction.commit_count.getAndIncrement();
//
//        Set<Map.Entry<K,ObjectDb<V>>> entrySet =  txn.writeSet.entrySet();
//        for (Map.Entry<K,ObjectDb<V>> obj : entrySet){
//            ObjectDb objectDb = obj.getValue();
//            objectDb.setOld();
//
//        }
//        unlock_locks(txn.readSet, txn.writeSet);
//        txn.isActive = false;
//        txn.success = true;
//
//        return true;
//    }
//
//    public void txn_abort(Transaction txn){
//        Set<Map.Entry<K,ObjectDb<V>>> entrySet =  txn.writeSet.entrySet();
//        for (Map.Entry<K,ObjectDb<V>> obj : entrySet){
//            ObjectDb objectDb = obj.getValue();
//            if (objectDb.isNew()){
//                concurrentHashMap.remove(obj.getKey());
//            } else {
//                objectDb.getObjectDb().setValue(objectDb.getValue());
//            }
//        }
//
//        unlock_locks(txn.readSet, txn.writeSet);
//        txn.isActive = false;
//        txn.success = false;
//        txn.commit_id = -1;
//    }
//
//    private void unlock_locks(Map<K, ObjectDb<?>> readSet, Map<K, ObjectDb<?>> writeSet){
//        for(ObjectDb objectDb : readSet.values()){
//            objectDb.unlock_read();
//        }
//        for(ObjectDb objectDb : writeSet.values()){
//            objectDb.unlock_write();
//        }
//    }

    /*
     * Public
     */

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
