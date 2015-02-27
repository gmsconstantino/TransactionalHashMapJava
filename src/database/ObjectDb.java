package database;

import structures.RwLock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by gomes on 26/02/15.
 */
public class ObjectDb<K,V> {

    K key;
    V value;

    private RwLock rwlock;
    private boolean isWrite; // 0 - read || 1 - write
    private Thread writeThread;

//    ConcurrentHashMap<>

    ObjectDb(Transaction t, K key, V value){
        rwlock = new RwLock();
        rwlock.lock_write();

        isWrite = true;
        writeThread = Thread.currentThread();

        this.key = key;
        this.value = value;

        t.addObjectDb(key, this);
    }


    public boolean try_lock_write_for(Transaction t, long time, TimeUnit unit){

        if(rwlock.lock.isWriteLockedByCurrentThread()){
            return true;
        } else if(rwlock.lock.getReadHoldCount() > 0){
            return upgrade_lock();
        } else if (rwlock.try_lock_write_for(time, unit)){
            isWrite = true;
            writeThread = Thread.currentThread();
            t.addObjectDb(key, this);
            return true;
        }
        return false;
    }

    public boolean try_lock_read_for(Transaction t, long time, TimeUnit unit){
        if (rwlock.try_lock_read_for(time, unit)){
            isWrite = false;
            writeThread = null;
            t.addObjectDb(key, this);
            return true;
        }
        return false;
    }

    public boolean upgrade_lock() throws IllegalMonitorStateException {
        if (rwlock.lock.getWriteHoldCount()>0){
            return true;
        } else if(rwlock.lock.getReadHoldCount() > 0){
            return rwlock.upgrade_lock();
        } else {
            throw new IllegalMonitorStateException();
        }
    }

    public synchronized void unlock() throws IllegalMonitorStateException {
        if (isWrite && Thread.currentThread() == writeThread){
            rwlock.unlock_write();
            isWrite = false;
            writeThread = null;
        } else if(isWrite){
            throw new IllegalMonitorStateException();
        } else {
            rwlock.unlock_read();
            isWrite = false;
            writeThread = null;
        }
    }


}
