package fct.thesis.databaseOCC;

import fct.thesis.database.ObjectLockDb;
import fct.thesis.structures.RwLock;

import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 26/02/15.
 */
public class ObjectVersionLockDBImpl<K,V> implements ObjectVersionLockDB<K,V> {

    V value;
    long version;

    private boolean isNew;
    private RwLock rwlock;

    public ObjectVersionLockDBImpl(V value){
        rwlock = new RwLock();
        rwlock.lock_write();

        version = ObjectLockDb.timestamp.getAndIncrement();
        isNew = true;
        this.value = value;
    }


    public boolean try_lock_write_for(long time, TimeUnit unit){
        if(rwlock.lock.isWriteLockedByCurrentThread()){
            return true;
        }
        return rwlock.try_lock_write_for(time, unit);
    }

    public boolean try_lock_read_for(long time, TimeUnit unit){
        return rwlock.try_lock_read_for(time, unit);
    }

    public void unlock_read() throws IllegalMonitorStateException {
        rwlock.unlock_read();
    }

    public void unlock_write() throws IllegalMonitorStateException {
        rwlock.unlock_write();
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
        return "ObjectVersionLockDBImpl{" +
                "value=" + value +
                ", version=" + version +
                ", isNew=" + isNew +
                ", rwlock=" + rwlock +
                '}';
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public void setValue(V value) {
        this.value = value;
        version = ObjectLockDb.timestamp.getAndIncrement();
    }

    @Override
    public ObjectLockDb getObjectDb() {
        return this;
    }
}
