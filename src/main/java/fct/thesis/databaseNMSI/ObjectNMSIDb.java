package fct.thesis.databaseNMSI;

import fct.thesis.database.ObjectDb;
import fct.thesis.structures.RwLock;
import pt.dct.util.P;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by gomes on 23/03/15.
 */
public class ObjectNMSIDb<K,V> implements ObjectDb<K,V> {

    AtomicLong version;
    volatile long minversion;
    ConcurrentHashMap<Long, P<V, AtomicInteger>> objects;
    ConcurrentHashMap<Transaction, Long> snapshots;

    RwLock rwlock;

    public ObjectNMSIDb(){
        version = new AtomicLong(-1L);
        minversion = 0;

        objects = new ConcurrentHashMap<>();
        snapshots = new ConcurrentHashMap<>();

        rwlock = new RwLock();
    }

    public Long getLastVersion() {
        return version.get();
    }

    public void putSnapshot(Transaction t, Long v) {
        if (v < minversion)
            return;

        objects.get(v).s.incrementAndGet();
        snapshots.put(t, v);
    }

    public Long getVersionForTransaction(Long tid){
        return snapshots.get(tid);
    }

    public V getValueVersion(long version, Set<Transaction> aggrDataTx) {
        if (version > getLastVersion() && version < minversion)
            return null;

        V value = objects.get(version).f;

//        for(P<Long, V> pair : objects){
//            if(pair.f <= version){
//                value = pair.s;
//                break;
//            }
//        }

        // Add tids to transaction metadata
        for (Map.Entry<Transaction, Long> entry : snapshots.entrySet()) {
            Long v = snapshots.get(entry.getKey());
            if(entry.getKey().isActive()) {
                if (v != null && v < version)
                    aggrDataTx.add(entry.getKey());
            } else {
                try {
                    objects.get(v).s.decrementAndGet();
                } catch (NullPointerException e){
//                    System.out.println("null version: "+v); // O v esta null por isso a excepcao
                }
                snapshots.remove(entry.getKey());
            }
        }

        return value;
    }

    /**
     * Add object with value and increment the version
     * Ocorre quando a transacao tem o write lock sobre o objecto
     * @param value
     */
    @Override
    public void setValue(V value) {
//        ObjectVersionLockDB<K,V> obj = new ObjectVersionLockDBImpl<K,V>(value);
//        obj.unlock_write();
        objects.putIfAbsent(version.incrementAndGet(), new P(value, new AtomicInteger(0)));
    }

    @Override
    public void clean(long version) {
        if (objects.size()<2)
            return;


        for (Map.Entry<Long,P<V, AtomicInteger>> entry : objects.entrySet()) {
            P<V, AtomicInteger> pair = entry.getValue();
            int counter = pair.s.get();
            if (entry.getKey() < getLastVersion()-1 && counter < 1){
                minversion = entry.getKey()+1;
                objects.remove(entry.getKey());
            }
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