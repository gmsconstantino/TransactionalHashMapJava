package database;

import structures.RwLock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by gomes on 26/02/15.
 */
public class ObjectDbImpl<V> implements ObjectDb<V> {

    V value;

    private RwLock rwlock;

    ObjectDbImpl(V value){
        rwlock = new RwLock();
        rwlock.lock_write();

//        this.value = value; Nao pode escrever o valor porque pode abortar a transacao
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

//    public boolean upgrade_lock() throws IllegalMonitorStateException {
//        if (rwlock.lock.getWriteHoldCount()>0){
//            return true;
//        } else if(rwlock.lock.getReadHoldCount() > 0){
//            return rwlock.upgrade_lock();
//        } else {
//            throw new IllegalMonitorStateException();
//        }
//    }

    public synchronized void unlock_read() throws IllegalMonitorStateException {
        rwlock.unlock_read();
    }

    public synchronized void unlock_write() throws IllegalMonitorStateException {
        rwlock.unlock_write();
    }

    @Override
    public String toString() {
        return "ObjectDb{" +
                "value=" + value +
                '}';
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public void setValue(V value) {
        this.value = value;
    }
}
