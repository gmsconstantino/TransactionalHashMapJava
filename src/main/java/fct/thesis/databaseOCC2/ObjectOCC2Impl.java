package fct.thesis.databaseOCC2;

import fct.thesis.database.ObjectLockDb;
import fct.thesis.structures.RwLock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by gomes on 26/02/15.
 */
public class ObjectOCC2Impl<K,V> implements ObjectOCC2<K,V> {

    V value;
    AtomicLong version;

    private RwLock rwlock;

    public ObjectOCC2Impl(V value){
        rwlock = new RwLock();
        version = new AtomicLong(-1L);
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

    public void lock_read(){
        rwlock.lock_read();
    }

    public void unlock_read() throws IllegalMonitorStateException {
        rwlock.unlock_read();
    }

    public void unlock_write() throws IllegalMonitorStateException {
        rwlock.unlock_write();
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
        return "ObjectVersionLockDBImpl{" +
                "value=" + value +
                ", version=" + version +
                ", rwlock=" + rwlock +
                '}';
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public long getVersion() {
        return version.get();
    }

    @Override
    public void setValue(V value) {
        this.value = value;
        version.incrementAndGet();
    }

    @Override
    public ObjectLockDb getObjectDb() {
        return this;
    }
}
