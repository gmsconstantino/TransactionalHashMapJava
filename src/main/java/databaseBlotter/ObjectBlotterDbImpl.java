package databaseBlotter;

import database.ObjectDb;
import databaseOCC.ObjectVersionLockDB;
import databaseOCC.ObjectVersionLockDBImpl;
import databaseOCCMulti.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import structures.RwLock;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by gomes on 23/03/15.
 */
public class ObjectBlotterDbImpl<K,V> implements ObjectBlotterDb<K,V> {

    private static final Logger logger = LoggerFactory.getLogger(ObjectBlotterDbImpl.class);
    boolean isDebug = logger.isDebugEnabled();

    AtomicLong version;

    LinkedList<Pair<Long, ObjectVersionLockDB<K,V>>> objects; //Devia remover objectos antigos
    ConcurrentHashMap<Long, Long> snapshots; //Devia ter ttl
    ConcurrentHashMap<Long,List<Long>> versionToTid; // version -> tid das transacoes

    RwLock rwlock;

    public ObjectBlotterDbImpl(){
        version = new AtomicLong(-1L);

        objects = new LinkedList<Pair<Long, ObjectVersionLockDB<K,V>>>();
        snapshots = new ConcurrentHashMap<Long,Long>();// tid -> version
        versionToTid = new ConcurrentHashMap<Long,List<Long>>(); // version -> tid das transacoes

        rwlock = new RwLock();
        rwlock.lock_write();
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
    public void putSnapshot(long tid, Long v) {
        snapshots.put(tid,v);
        versionToTid.get(v).add(tid);
    }

    public Long getVersionForTransaction(long tid){
        return snapshots.get(tid);
    }

    public Pair<V,ListIterator<Long>> getValueVersion(long version) {
        if (version > getLastVersion())
            return null;

        Pair<V,ListIterator<Long>> p = new Pair<V,ListIterator<Long>>();

        for(Pair<Long, ObjectVersionLockDB<K,V>> pair : objects){
            if(pair.f <= version){
                p.f = pair.s.getValue();
                break;
            }
        }

        p.s = versionToTid.get(version).listIterator();
        return p;
    }

    /**
     * Add object with value and increment the version
     * @param value
     */
    @Override
    public void setValue(V value) {
        ObjectVersionLockDB<K,V> obj = new ObjectVersionLockDBImpl<K,V>(value);
        obj.unlock_write();
        objects.addFirst(new Pair(version.incrementAndGet(), obj));

        LinkedList<Long> list = new LinkedList<Long>(versionToTid.get(version.get()-1));
        versionToTid.put(getLastVersion(), Collections.synchronizedList(list));
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
    public void lock_read() {
        rwlock.lock_read();
    }

    @Override
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

    public synchronized void unlock_read() throws IllegalMonitorStateException {
        rwlock.unlock_read();
    }

    public synchronized void unlock_write() throws IllegalMonitorStateException {
        rwlock.unlock_write();
    }

    @Override
    public String toString() {
        return "ObjectMultiVersionDB{" +
                "last_version=" + version.get() +
                ", objects=" + objects +
                '}';
    }

}