package fct.thesis.database2PL;

import fct.thesis.database.ObjectLockDb;
import fct.thesis.structures.RwLock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;

/**
 * Created by gomes on 26/02/15.
 */
public class ObjectLockDbImpl<K,V> extends ObjectLockDbAbs<K,V> {

    private StampedLock lock;

    ObjectLockDbImpl(V value){
        super(value);
        lock = new StampedLock();
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

    @Override
    public String toString() {
        return "ObjectDbImpl{" +
                "value=" + value +
                ", isNew=" + isNew +
                ", lock=" + lock +
                '}';
    }
}
