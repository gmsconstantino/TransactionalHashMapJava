package fct.thesis.database2PL;

import fct.thesis.database.ObjectDb;
import fct.thesis.database.ObjectLockDb;

import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 22/04/15.
 */
public class ObjectLockDbAbs<K,V> implements ObjectLockDb<K,V> {

    V value;
    boolean isNew;

    public ObjectLockDbAbs(V value) {
        this.value = value;
        this.isNew = true;
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
    public boolean try_lock_write_for(long time, TimeUnit unit) {
        return false;
    }

    @Override
    public boolean try_lock_read_for(long time, TimeUnit unit) {
        return false;
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
}
