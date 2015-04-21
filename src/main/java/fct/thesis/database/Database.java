package fct.thesis.database;

import fct.thesis.structures.MapEntry;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by gomes on 26/02/15.
 */
public class Database<K,V> {

    public static AtomicLong timestamp = new AtomicLong(1);
//    public ConcurrentHashMap<K, ObjectDb<K,V>> concurrentHashMap;
    public ObjectDb<K,V>[] db;


    public Database(){
//        concurrentHashMap = new ConcurrentHashMap<K, ObjectDb<K,V>>(1000000,0.75f,64);
        db = (ObjectDb<K,V>[]) new Object[10000000];
    }

    protected ObjectDb<K,V> getKey(K key){
//        return concurrentHashMap.get(key);
        return db[(Integer)key];
    }

    protected ObjectDb<K,V> putIfAbsent(K key, ObjectDb<K,V> obj){
//        return concurrentHashMap.putIfAbsent(key, obj);
        return db[(Integer) key];
    }

    protected ObjectDb<K,V> removeKey(K key){
//        return concurrentHashMap.remove(key);
        return null;
    }



    /*
     * Public
     */

    public Transaction<K,V> newTransaction(TransactionFactory.type t){
        return TransactionFactory.createTransaction(t, this);
    }

    public int size(){
//        return concurrentHashMap.size();
        return 10000000;
    }
//
//    public Iterator getIterator() {
//        return new DbIterator();
//    }
//
//    private class DbIterator implements Iterator {
//
//        Set<Map.Entry<K, ObjectDb<K,V>>> set;
//        Iterator<Map.Entry<K, ObjectDb<K,V>>> it;
//        DbIterator(){
//            set = concurrentHashMap.entrySet();
//            it = set.iterator();
//        }
//
//        @Override
//        public boolean hasNext() {
//            return it.hasNext();
//        }
//
//        @Override
//        public Object next() {
//
//            if(this.hasNext()){
//                Map.Entry<K, ObjectDb<K,V>> obj = it.next();
//                ObjectDb<K,V> objectDb = obj.getValue();
//                return new MapEntry<K,V>(obj.getKey(),objectDb.getValue());
//            }
//            return null;
//        }
//
//        @Override
//        public void remove() {
//
//        }
//    }

}
