package fct.thesis.storage;

import fct.thesis.database.ObjectDb;
import fct.thesis.database.Storage;
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
            tables[i] = new ConcurrentHashMap<>(1000000,0.75f,256);
        }
    }

    @Override
    public V getKey(int table, K key) {
        return (V) tables[table].get(key);
    }

    @Override
    public V putIfAbsent(int table, K key, V obj) {
        return (V) tables[table].putIfAbsent(key, obj);
    }

    @Override
    public Iterator<V> getIterator(int table) {
        return new StorageIterator(table);
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

    private class StorageIterator implements Iterator {

        //Set<Map.Entry<K, V>> set;
        //Iterator<Map.Entry<K, V>> it;
        Iterator it;
        StorageIterator(int table){
            //set = tables[table].entrySet();
            //it = set.iterator();
            it = tables[table].values().iterator();
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public Object next() {

            /*
            if(this.hasNext()){
                Map.Entry<K, V> obj = it.next();
                return obj.getValue();
            }
            return null;*/
            return it.next();
        }

        @Override
        public void remove() {

        }
    }

}
