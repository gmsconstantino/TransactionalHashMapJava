package fct.thesis.databaseNMSI;

import fct.thesis.database.ObjectDb;
import fct.thesis.structures.RwLock;
import pt.dct.util.P;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by gomes on 23/03/15.
 */
public class ObjectNMSIDb<K,V> implements ObjectDb<K,V> {

    AtomicLong version;
    ConcurrentLinkedDeque<P<Long, V>> objects;
    ConcurrentHashMap<Transaction, Long> snapshots;

    RwLock rwlock;

    public ObjectNMSIDb(){
        version = new AtomicLong(-1L);

        objects = new ConcurrentLinkedDeque<>();
        snapshots = new ConcurrentHashMap<>();

        rwlock = new RwLock();
    }

    public Long getLastVersion() {
        return version.get();
    }

    public Long incrementAndGetVersion() {
        return version.incrementAndGet();
    }

    public void putSnapshot(Transaction t, Long v) {
        snapshots.put(t, v);
    }

//    public void removeSnapshot(Long tid) {
//        snapshots.remove(tid);
//    }

    public Long getVersionForTransaction(Long tid){
        return snapshots.get(tid);
    }

    public V getValueVersion(long version, Set<Transaction> aggrDataTx) {
        if (version > getLastVersion())
            return null;

        V value = null;

        for(P<Long, V> pair : objects){
            if(pair.f <= version){
                value = pair.s;
                break;
            }
        }

        // Add tids to transaction metadata
        for (Transaction transaction : snapshots.keySet()) {
            if(transaction.isActive()) {
                Long v = snapshots.get(transaction);
                if (v != null && v < version)
                    aggrDataTx.add(transaction);
            }
        }

        return value;
    }

    /**
     * Add object with value and increment the version
     * @param value
     */
    @Override
    public void setValue(V value) {
//        ObjectVersionLockDB<K,V> obj = new ObjectVersionLockDBImpl<K,V>(value);
//        obj.unlock_write();
        objects.addFirst(new P(version.incrementAndGet(), value));
    }

    public long getMin() {
        long min = Long.MAX_VALUE;
        long i;
        for (Transaction transaction : snapshots.keySet()) {
            i = snapshots.get(transaction);
            if (transaction.isActive()) {
                if (i < min) {
                    min = i;
                }
            } else
                snapshots.remove(transaction);

        }
        return min;
    }

    @Override
    public void clean(long version) {
        if (objects.size()<2)
            return;

        version = getMin();

        Iterator<P<Long, V>> it = objects.descendingIterator();
        while (it.hasNext()){
            P<Long, V> pair = it.next();
            if (version > pair.f)
                objects.removeLastOccurrence(pair);
            else
                break;

            if (objects.size()==1)
                return;
        }
    }

    @Override
    public ObjectDb getObjectDb() {
        return this;
    }

    @Override
    public V getValue() {
        return null;
    }

    public void lock_read() {
        rwlock.lock_read();
    }

    public void lock_write() {
        rwlock.lock_write();
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
    public String toString() {
        return "ObjectBlotterDbImpl@" + hashCode()+" {" +
                "objects=" + objects +
                ", snapshots=" + snapshots +
                '}';
    }
}