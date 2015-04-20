package fct.thesis.databaseOCC2;

import fct.thesis.database.ObjectDb;
import fct.thesis.database.TransactionAbortException;
import pt.dct.util.P;

import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 28/02/15.
 */
public class BufferOCC2<K,V> implements ObjectOCC2<K,V> {

    K key;
    V value;
    long version;
    ObjectDb<K,V> objectDb;
    private boolean isNew;

    public BufferOCC2(V value, ObjectDb<K, V> obj) {
        this.value = value;
        this.objectDb = obj;
        isNew = false;
    }

    public BufferOCC2(K key, V value, ObjectDb<K, V> obj) {
        this.key = key;
        this.value = value;
        this.objectDb = obj;
        isNew = false;
    }

    public BufferOCC2(V value, long version, ObjectDb<K, V> obj) {
        this.value = value;
        this.version = version;
        this.objectDb = obj;
        isNew = false;
    }

    public BufferOCC2(K key, V value, long version, ObjectDb<K, V> obj) {
        this.key = key;
        this.value = value;
        this.version = version;
        this.objectDb = obj;
        isNew = false;
    }

    public BufferOCC2(V value, long version, ObjectDb<K, V> obj, boolean isNew) {
        this.value = value;
        this.version = version;
        this.objectDb = obj;
        this.isNew = isNew;
    }

    public BufferOCC2(K key, V value, long version, ObjectDb<K, V> obj, boolean isNew) {
        this.key = key;
        this.value = value;
        this.version = version;
        this.objectDb = obj;
        this.isNew = isNew;
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

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public void setValue(V value) {
        this.value = value;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public ObjectDb<K,V> getObjectDb() {
        return objectDb;
    }

    @Override
    public V readVersion(long version) throws TransactionAbortException {
        return null;
    }

    @Override
    public P<V, Long> readLast() throws TransactionAbortException {
        return null;
    }

    @Override
    public boolean try_lock(long time, TimeUnit unit) {
        return false;
    }

    @Override
    public void unlock() {

    }
}
