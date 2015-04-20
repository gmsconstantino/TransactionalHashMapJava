package fct.thesis.databaseOCC2;

import fct.thesis.database.TransactionAbortException;
import pt.dct.util.P;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by gomes on 26/02/15.
 */
public class ObjectOCC2Impl<K,V> implements ObjectOCC2<K,V> {

    volatile V value;
    volatile long version;

    volatile Object monitor;

    private ReentrantLock lock;

    public ObjectOCC2Impl(V value){
        lock = new ReentrantLock();

        monitor = new Object();
        version = -1L;
        this.value = value;
    }

    public boolean try_lock(long time, TimeUnit unit){
        try {
            return lock.tryLock(time, unit);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public void unlock() {
        for (int i = 0; i < lock.getHoldCount()+1; i++) {
            lock.unlock();
        }
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public long getVersion() {
        long v = -1L;
        synchronized (monitor){
            v = version;
        }
        return version;
    }

    @Override
    public V readVersion(long v) throws TransactionAbortException {
        V value = null;
        synchronized (monitor){
            if (v==version)
                value = this.value;
        }
        if (value==null)
            throw new TransactionAbortException("ReadVersion: Version Change");
        return value;
    }

    @Override
    public P<V, Long> readLast() throws TransactionAbortException {
        Long version = getVersion();
        V v = readVersion(version);
        return new P<V, Long>(value, version);
    }

    @Override
    public void setValue(V value) {
        synchronized (monitor){
            this.value = value;
            ++version;
        }
    }

    @Override
    public ObjectOCC2 getObjectDb() {
        return this;
    }
}
