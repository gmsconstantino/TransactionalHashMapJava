package database;

import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 28/02/15.
 */
public class WriteBufferObjectDb<V> implements ObjectDb<V> {

    V value;
    ObjectDb<V> objectDb;

    public WriteBufferObjectDb(V value, ObjectDb<V> obj) {
        this.value = value;
        this.objectDb = obj;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public long getVersion() {
        return objectDb.getVersion();
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
        return false;
    }

    @Override
    public void setOld() {

    }

    @Override
    public String toString() {
        return "CacheObjectDb{" +
                "value=" + value +
                '}';
    }
}
