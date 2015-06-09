package fct.thesis.databaseNMSI;

import fct.thesis.database.*;
import fct.thesis.structures.RwLock;
import pt.dct.util.P;

import java.util.Map;
import java.util.Random;
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
    long minversion;
    public ConcurrentHashMap<Long, V> objects;
    public ConcurrentHashMap<Transaction, Long> snapshots;

    RwLock rwlock;

    long id;
    static AtomicLong c = new AtomicLong(0);

    public ObjectNMSIDb(){
        version = new AtomicLong(-1L);
        minversion = 0;

        objects = new ConcurrentHashMap<>();
        snapshots = new ConcurrentHashMap<>();

        rwlock = new RwLock();

        id = c.getAndIncrement();
    }

    public Long getLastVersion() {
        return version.get();
    }

    public void putSnapshot(Transaction t, Long v) {
//        if (v < minversion)
//            return;

        snapshots.put(t, v);
    }

    public Long getVersionForTransaction(Transaction tid){
        return snapshots.get(tid);
    }

    public V getValueVersion(long version, Set<Transaction> aggrDataTx) {
        if (version > getLastVersion() && version < minversion)
            return null;

        V value = null;
        value = objects.get(version);

        // Add tids to transaction metadata
        for (Map.Entry<Transaction, Long> entry : snapshots.entrySet()) {
            Long v = snapshots.get(entry.getKey());
            if(entry.getKey().isActive()) {
                if (v != null && v < version)
                    aggrDataTx.add(entry.getKey());
            } else {
//                try {
                    snapshots.remove(entry.getKey());
//                } catch (NullPointerException e){
//                    System.out.println("null version: "+v); // O v esta null por isso a excepcao
//                }

            }
        }

        return value;
    }

//    public static void addToCleaner(ObjectNMSIDb o, Long version) {
//        Database.asyncPool.execute(() -> {
//            o.clean(version);
//            try {
//                ThreadCleanerNMSI.set.add(new P<>(o,version));
//            } catch (Exception e) {
//            }
//        });
//    }

    /**
     * Add object with value and increment the version
     * Ocorre quando a transacao tem o write lock sobre o objecto
     * @param value
     */
    @Override
    public void setValue(V value) {
//        ObjectVersionLockDB<K,V> obj = new ObjectVersionLockDBImpl<K,V>(value);
//        obj.unlock_write();
        long newversion = version.incrementAndGet();
        objects.putIfAbsent(newversion, value);
    }

    @Override
    public void clean(long version) {
        if (objects.size()<1)
            return;

        long myminversion = Long.MAX_VALUE;

        for (Map.Entry<Transaction, Long> entry : snapshots.entrySet()) {
            myminversion = Math.min(myminversion, entry.getValue());
        }

        for (Map.Entry<Long,V> entry : objects.entrySet()) {
            if (entry.getKey() < myminversion){
                objects.remove(entry.getKey());
            }
        }

        minversion = myminversion;
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