package fct.thesis.database;

import fct.thesis.database.ObjectDb;
import fct.thesis.database.ObjectLockDb;

import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 22/04/15.
 */
public class ObjectLockDbAbstract<V> implements ObjectLockDb<V> {

    private V value;

    public ObjectLockDbAbstract() {
    }

    public ObjectLockDbAbstract(V value) {
        this.value = value;
    }

    @Override
    public ObjectLockDb getObjectDb() {
        return this;
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
    public void clean(long version) {

    }

    @Override
    public boolean try_lock_write_for(long time, TimeUnit unit) {
        return false;
    }

    @Override
    public boolean try_lock_read_for(long time, TimeUnit unit) {
        return false;
    }

    @Override
    public void lock_write() {

    }

    @Override
    public void lock_read() {

    }

    @Override
    public void unlock_read() {

    }

    @Override
    public void unlock_write() {

    }

    @Override
    public long getVersion() {
        return -1L;
    }

}
