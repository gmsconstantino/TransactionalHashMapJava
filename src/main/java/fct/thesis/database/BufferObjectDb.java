package fct.thesis.database;

import fct.thesis.database.BufferDb;
import fct.thesis.database.ObjectDb;
import fct.thesis.database.ObjectLockDb;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 28/02/15.
 */
public class BufferObjectDb<K extends Comparable<K>,V> implements BufferDb<K,V>, Comparable {

    private int table;
    private K key;
    private V value;
    private long version;

    private ObjectDb<K,V> objectDb;

    public BufferObjectDb(V value){
        this.value = value;
        this.version = -1L;
    }
    
    public BufferObjectDb(int table, K key, V value){
        this.table = table;
        this.key = key;
        this.value = value;
        this.version = -1L;
    }

    public BufferObjectDb(int table, K key, V value, ObjectDb<K, V> obj) {
        this.table = table;
        this.key = key;
        this.value = value;
        this.objectDb = obj;
        this.version = -1L;
    }

    public BufferObjectDb(int table, K key, V value, long version, ObjectDb<K, V> obj) {
        this.table = table;
        this.key = key;
        this.value = value;
        this.version = version;
        this.objectDb = obj;
    }

    @Override
    public V getValue() {
        return value;
    }

    public K getKey() {
        return key;
    }

    @Override
    public int getTable() {
        return table;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public ObjectDb getObjectDb(){
        return objectDb;
    }

    public void setObjectDb(ObjectDb<K, V> objectDb) {
        this.objectDb = objectDb;
    }

    @Override
    public void setValue(V value) {
        this.value = value;
    }

    @Override
    public void clean(long version) {

    }

    @Override
    public boolean equals(Object obj) {
        BufferObjectDb<K,V> buffer = (BufferObjectDb<K,V>) obj;
        return key.equals(buffer.getKey());
    }

    @Override
    public String toString() {
        return "BufferObjectDb{" +
                "value=" + value +
                ", objectDb=" + objectDb +
                '}';
    }

    @Override
    public int compareTo(Object o) {
        BufferObjectDb<K,V> buffer = (BufferObjectDb<K,V>) o;
        return key.compareTo(buffer.getKey());
    }
}
