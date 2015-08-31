package fct.thesis.database;

import java.util.Iterator;

/**
 * Created by Constantino Gomes on 31/08/15.
 */
public interface Storage<K,V> {

    ObjectDb<K,V> getKey(int table, K key);

    ObjectDb<K,V> putIfAbsent(int table, K key, ObjectDb<K,V> obj);

    ObjectDb<K,V> removeKey(int table, K key);

    ObjectDb<K,V> getKey(K key);

    ObjectDb<K,V> putIfAbsent(K key, ObjectDb<K,V> obj);

    ObjectDb<K,V> removeKey(K key);

    Iterator<ObjectDb<K,V>> getObjectDbIterator(int table);

    Iterator<ObjectDb<K,V>> getObjectDbIterator();

    int getTablesNumber();

    int getSize();

    int getSizeTable(int table);

    void cleanup();

}
