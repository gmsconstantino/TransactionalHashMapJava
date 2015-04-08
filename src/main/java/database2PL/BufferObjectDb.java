package database2PL;

import database.ObjectDb;
import database.ObjectLockDb;

import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 28/02/15.
 */
public class BufferObjectDb<K,V> implements ObjectLockDb<K,V> {

    K key;
    V value;

    ObjectDb<K,V> objectDb;
    private boolean isNew;

    public BufferObjectDb(K key, V value){
        this.key = key;
        this.value = value;
        isNew=false;
    }

    public BufferObjectDb(V value, ObjectDb<K, V> obj) {
        this.value = value;
        this.objectDb = obj;
        isNew = false;
    }

    @Override
    public V getValue() {
        return value;
    }

    public K getKey() {
        return key;
    }

    @Override
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
                ", isNew=" + isNew +
                ", objectDb=" + objectDb +
                '}';
    }
}
