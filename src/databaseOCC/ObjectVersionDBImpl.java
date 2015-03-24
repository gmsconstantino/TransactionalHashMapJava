package databaseOCC;

import database.ObjectDb;
import structures.RwLock;

import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 26/02/15.
 */
public class ObjectVersionDBImpl<K,V> implements databaseOCC.ObjectVersionDB<K,V> {

    V value;
    long version;

    private boolean isNew;
    private RwLock rwlock;

    public ObjectVersionDBImpl(V value){
        rwlock = new RwLock();
        rwlock.lock_write();

        version = ObjectDb.timestamp.getAndIncrement();
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

    public synchronized void unlock_read() throws IllegalMonitorStateException {
        rwlock.unlock_read();
    }

    public synchronized void unlock_write() throws IllegalMonitorStateException {
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
        return "ObjectDbImpl{" +
                "value=" + value +
                ", version=" + version +
                ", isNew=" + isNew +
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
        version = ObjectDb.timestamp.getAndIncrement();
    }

    @Override
    public ObjectDb getObjectDb() {
        return this;
    }
}
