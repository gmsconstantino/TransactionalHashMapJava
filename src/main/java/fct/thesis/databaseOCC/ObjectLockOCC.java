package fct.thesis.databaseOCC;

import fct.thesis.database.ObjectLockDb;
import fct.thesis.database.ObjectLockDbAbstract;
import fct.thesis.structures.RwLock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by gomes on 26/02/15.
 */
public class ObjectLockOCC<K,V> extends ObjectLockDbAbstract<K,V> {

    AtomicLong versionGR = new AtomicLong(-1L);

    private volatile long version;
    private RwLock rwlock;

    public ObjectLockOCC(V value){
        super(value);
        rwlock = new RwLock();
        version = versionGR.getAndIncrement();
    }

    @Override
    public boolean try_lock_read_for(long time, TimeUnit unit) {
        return rwlock.try_lock_read_for(time, unit);
    }

    @Override
    public void lock_write() {
        rwlock.lock_write();
    }

    @Override
    public void lock_read() {
        rwlock.lock_read();
    }

    public void unlock_read() throws IllegalMonitorStateException {
        rwlock.unlock_read();
    }

    public void unlock_write() throws IllegalMonitorStateException {
        rwlock.unlock_write();
    }

    @Override
    public String toString() {
        return "ObjectLockOCC{" +
                "value=" + getValue() +
                ", version=" + version +
                ", rwlock=" + rwlock +
                '}';
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public void setValue(V value) {
        super.setValue(value);
        version = versionGR.getAndIncrement();
    }

    @Override
    public ObjectLockDb getObjectDb() {
        return this;
    }
}
