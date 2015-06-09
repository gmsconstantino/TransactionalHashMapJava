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

    static Random r = new Random();

    AtomicLong version;
    long minversion;
    public ConcurrentHashMap<Long, P<V, AtomicInteger>> objects;
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

        long c = objects.get(v).s.incrementAndGet();
        snapshots.put(t, v);
        System.out.println("Obj: "+id+" ver: "+v+" T " + t.id + " add c: "+c);
    }

    public Long getVersionForTransaction(Transaction tid){
        Long v = snapshots.get(tid);
        if (v!=null)
            System.out.println("Obj: "+id+" ver: "+v+" T " + tid.id + " GET");
        return v;
    }

    public V getValueVersion(long version, Set<Transaction> aggrDataTx) {
        if (version > getLastVersion() && version < minversion)
            return null;

        V value = null;
        try {
            value = objects.get(version).f;
        } catch (NullPointerException e){
            System.out.println("Obj: "+id+" ver: "+version+" Null");
//            e.printStackTrace();
            throw e;
        }

        // Add tids to transaction metadata
        for (Map.Entry<Transaction, Long> entry : snapshots.entrySet()) {
            Long v = snapshots.get(entry.getKey());
            if(entry.getKey().isActive()) {
                if (v != null && v < version)
                    aggrDataTx.add(entry.getKey());
            } else {
//                try {
                    if(snapshots.remove(entry.getKey()) != null) {
                        long c = objects.get(v).s.decrementAndGet();
                        System.out.println("Obj: "+id+" ver: " + v + " T " + entry.getKey().id + " dec c: " + c);
                        if (c == 0){
                            addToCleaner(this, v);
                        }
                    }
//                } catch (NullPointerException e){
//                    System.out.println("null version: "+v); // O v esta null por isso a excepcao
//                }

            }
        }

        return value;
    }

    public static void addToCleaner(ObjectNMSIDb o, Long version) {
        Database.asyncPool.execute(() -> {
            o.clean(version);
//            try {
//                ThreadCleanerNMSI.set.add(new P<>(o,version));
//            } catch (Exception e) {
//            }
        });
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
        long newversion = version.incrementAndGet();
        objects.putIfAbsent(newversion, new P(value, new AtomicInteger(0)));
        System.out.println("Obj: "+id+" ver: "+ newversion+" New");
    }

    @Override
    public void clean(long version) {
        if (objects.size()<2)
            return;

        if (version < this.version.get()-2 && objects.get(version)!=null) {
            if(objects.get(version).s.get() == 0){
                objects.remove(version);
                System.out.println("Obj: " + id + " ver: " + version + " Del");
            } else {
                addToCleaner(this, version);
            }
        }
//        long myminversion = Long.MIN_VALUE;
//        long mylastversion = getLastVersion()-1;
//
//        for (Map.Entry<Long,P<V, AtomicInteger>> entry : objects.entrySet()) {
//            P<V, AtomicInteger> pair = entry.getValue();
//            int counter = pair.s.get();
//            if (entry.getKey() < mylastversion && counter == 0){
//                myminversion = Math.max(entry.getKey() + 1, myminversion);
//                objects.remove(entry.getKey());
//            }
//        }
//
//        minversion = myminversion;

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