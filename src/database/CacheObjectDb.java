package database;

import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 28/02/15.
 */
public class CacheObjectDb<V> implements ObjectDb<V> {

    V value;

    public CacheObjectDb(V value) {
        this.value = value;
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
    public String toString() {
        return "CacheObjectDb{" +
                "value=" + value +
                '}';
    }
}
