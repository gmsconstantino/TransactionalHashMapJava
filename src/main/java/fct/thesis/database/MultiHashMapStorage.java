package fct.thesis.database;

import fct.thesis.structures.MapEntry;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Constantino Gomes on 31/08/15.
 */
public class MultiHashMapStorage<K,V> implements Storage<K,V> {

    private int numberTables;
    private ConcurrentHashMap[] tables;

    public MultiHashMapStorage() {
        this(1);
    }

    public MultiHashMapStorage(int ntables) {
        numberTables = ntables;
        tables = new ConcurrentHashMap[ntables];
        for (int i = 0; i < ntables; i++) {
            tables[i] = new ConcurrentHashMap<K,ObjectDb<K,V>>(1000000,0.75f,256);
        }
    }

    @Override
    public ObjectDb<K, V> getKey(int table, K key) {
        return (ObjectDb<K, V>) tables[table].get(key);
    }

    @Override
    public ObjectDb<K, V> putIfAbsent(int table, K key, ObjectDb<K, V> obj) {
        return (ObjectDb<K,V>) tables[table].putIfAbsent(key, obj);
    }

    @Override
    public ObjectDb<K, V> removeKey(int table, K key) {
        return (ObjectDb<K,V>) tables[table].remove(key);
    }

    @Override
    public ObjectDb<K, V> getKey(K key) {
        return getKey(0, key);
    }

    @Override
    public ObjectDb<K, V> putIfAbsent(K key, ObjectDb<K, V> obj) {
        return putIfAbsent(0, key, obj);
    }

    @Override
    public ObjectDb<K, V> removeKey(K key) {
        return removeKey(0, key);
    }

    @Override
    public Iterator<ObjectDb<K, V>> getObjectDbIterator(int table) {
        return new ObjectDbIterator(table);
    }

    @Override
    public Iterator<ObjectDb<K, V>> getObjectDbIterator() {
        return getObjectDbIterator(0);
    }

    @Override
    public int getTablesNumber() {
        return numberTables;
    }

    @Override
    public int getSize() {
        return tables[0].size();
    }

    @Override
    public int getSizeTable(int table) {
        return tables[table].size();
    }

    @Override
    public void cleanup() {

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
