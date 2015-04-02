package databaseBlotter;

import database.ObjectDb;
import databaseOCC.ObjectVersionLockDB;
import databaseOCC.ObjectVersionLockDBImpl;
import databaseOCCMulti.Pair;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by gomes on 23/03/15.
 */
public class ObjectBlotterDbImpl<K,V> implements ObjectBlotterDb<K,V> {

    AtomicLong version;

    LinkedList<Pair<Long, ObjectVersionLockDB<K,V>>> objects;
    ConcurrentHashMap<Long, Long> snapshots; //Devia ter ttl

    ReentrantLock lock;

    public ObjectBlotterDbImpl(){
        version = new AtomicLong(-1L);

        objects = new LinkedList<>();
        snapshots = new ConcurrentHashMap<>();

        lock = new ReentrantLock();
    }

    @Override
    public Long getLastVersion() {
        return version.get();
    }

    @Override
    public Long incrementAndGetVersion() {
        return version.incrementAndGet();
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) {
        try {
            return lock.tryLock(timeout, unit);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void putSnapshot(long id, Long v) {
        snapshots.put(id,v);
    }

    public Long getVersionForTransaction(long id){
        return snapshots.get(id);
    }

    public Pair<V,List<Long>> getValueVersion(long version) {
        Pair<V,List<Long>> p = new Pair<V,List<Long>>();
        p.s = new LinkedList<>();

        for(Pair<Long, ObjectVersionLockDB<K,V>> pair : objects){
            if(p.f==null && pair.f <= version){
                p.f = pair.s.getValue();
                break;
            }
        }

        if (p.f != null){
            for (Long tid : snapshots.keySet()){
                if (snapshots.get(tid) < version)
                    p.s.add(tid);
            }
            return p;
        }

        return null;
    }

    /**
     * Add object with value and increment the version
     * @param value
     */
    @Override
    public void setValue(V value) {
        ObjectVersionLockDB<K,V> obj = new ObjectVersionLockDBImpl<>(value);
        objects.addFirst(new Pair(version.incrementAndGet(), obj));
        obj.unlock_write();

    }

    @Override
    public ObjectDb getObjectDb() {
        return this;
    }

    @Override
    public V getValue() {
        return null;
    }

    @Override
    public String toString() {
        return "ObjectMultiVersionDB{" +
                "last_version=" + version.get() +
                ", objects=" + objects +
                '}';
    }

}