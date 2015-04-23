package fct.thesis.databaseOCC;

import fct.thesis.database.ObjectLockDb;
import fct.thesis.database.ObjectLockDbAbstract;
import fct.thesis.structures.RwLock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

/**
 * Created by gomes on 26/02/15.
 */
public class ObjectLockOCC<K,V> extends ObjectLockDbAbstract<K,V> {

    AtomicLong versionGR = new AtomicLong(-1L);

    private long version;
    private StampedLock lock;

    public ObjectLockOCC(V value){
        super(value);
        lock = new StampedLock();
        version = versionGR.getAndIncrement();
    }

    public long lockStamp_write() {
        return lock.writeLock();
    }

    public long lockStamp_read() {
        return lock.readLock();
    }

    public long tryOptimisticRead(){
        return lock.tryOptimisticRead();
    }

    public boolean validate(long stamp){
        return lock.validate(stamp);
    }

    public void unlock(long stamp) {
        lock.unlock(stamp);
    }

    @Override
    public String toString() {
        return "ObjectVersionLockDBImpl{" +
                "value=" + getValue() +
                ", version=" + version +
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
