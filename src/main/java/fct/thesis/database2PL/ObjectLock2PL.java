package fct.thesis.database2PL;

import fct.thesis.database.ObjectLockDbAbstract;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;

/**
 * Created by gomes on 26/02/15.
 */
public class ObjectLock2PL<K,V> extends ObjectLockDbAbstract<K,V> {

    private StampedLock lock;
    private boolean isNew;

    ObjectLock2PL(V value){
        super(value);
        lock = new StampedLock();
        isNew = true;
    }

    public long try_lock_write(long time, TimeUnit unit){
        try {
            return lock.tryWriteLock(time,unit);
        } catch (InterruptedException e) {
            return 0L;
        }
    }

    public long try_lock_read(long time, TimeUnit unit){
        try {
            return lock.tryReadLock(time, unit);
        } catch (InterruptedException e) {
            return 0L;
        }
    }

    public long try_upgrade(long stamp){
        return lock.tryConvertToWriteLock(stamp);
    }

    public void unlock_read(long stamp) {
        lock.unlockRead(stamp);
    }

    public void unlock_write(long stamp) {
        lock.unlockWrite(stamp);
    }

    public void unlock(Long stamp) {
        lock.unlock(stamp);
    }

    public boolean isNew() {
        return isNew;
    }

    public void setOld() {
        isNew = false;
    }

    @Override
    public String toString() {
        return "ObjectLock2PL{" +
                "value=" + getValue() +
                ", isNew=" + isNew +
                ", lock=" + lock +
                '}';
    }
}
