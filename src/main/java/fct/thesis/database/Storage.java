package fct.thesis.database;

import java.util.Iterator;

/**
 * Created by Constantino Gomes on 31/08/15.
 */
public interface Storage<K,V> {

    V getKey(int table, K key);

    V putIfAbsent(int table, K key, V obj);

    Iterator<V> getIterator(int table);

    int getTablesNumber();

    int getSize();

    int getSizeTable(int table);

    void cleanup();

}
