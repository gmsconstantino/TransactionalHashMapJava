package fct.thesis.databaseOCC;

import fct.thesis.database.ObjectDb;

import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 28/02/15.
 */
public class BufferObjectVersionDB<K,V> implements ObjectVersionLockDB<K,V> {

    K key;
    V value;
    long version;
    ObjectDb<K,V> objectDb;
    private boolean isNew;

    public BufferObjectVersionDB(V value, ObjectDb<K, V> obj) {
        this.value = value;
        this.objectDb = obj;
        isNew = false;
    }

    public BufferObjectVersionDB(V value, long version, ObjectDb<K, V> obj) {
        this.value = value;
        this.version = version;
        this.objectDb = obj;
        isNew = false;
    }

    public BufferObjectVersionDB(V value, long version, ObjectDb<K, V> obj, boolean isNew) {
        this.value = value;
        this.version = version;
        this.objectDb = obj;
        this.isNew = isNew;
    }

    public BufferObjectVersionDB(K key, V value, long version, ObjectDb<K, V> obj, boolean isNew) {
        this.key = key;
        this.value = value;
        this.version = version;
        this.objectDb = obj;
        this.isNew = isNew;
    }

    @Override
    public V getValue() {
        return value;
    }

    public K getKey() {
        return key;
    }

    @Override
    public long getVersion() {
        return version;
    }

    public ObjectDb getObjectDb(){
        return objectDb;
    }

    @Override
    public void setValue(V value) {
        this.value = value;
    }

    @Override
    public boolean try_lock_write_for(long time, TimeUnit unit) {
        return true;
    }

    @Override
    public boolean try_lock_read_for(long time, TimeUnit unit) {
        return true;
    }

    @Override
    public void unlock_read() {

    }

    @Override
    public void unlock_write() {

    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    @Override
    public void setOld() {
        isNew = false;
    }

    @Override
    public String toString() {
        return "BufferObjectDb{" +
                "value=" + value +
                ", version=" + version +
                ", isNew=" + isNew +
                ", objectDb=" + objectDb +
                '}';
    }
}
