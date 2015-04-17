package fct.thesis.databaseBlotter;

import fct.thesis.database.ObjectDb;
import fct.thesis.databaseOCCMulti.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import fct.thesis.structures.RwLock;

import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by gomes on 23/03/15.
 */
public class ObjectBlotterDbImpl<K,V> implements ObjectBlotterDb<K,V> {

    private static final Logger logger = LoggerFactory.getLogger(ObjectBlotterDbImpl.class);
    boolean isDebug = logger.isDebugEnabled();

    AtomicLong version;

    LinkedList<Pair<Long, V>> objects; //Devia remover objectos antigos

    ConcurrentHashMap<Long, Long> snapshots; //Devia ter ttl
//    public Cache<Long, Long> snapshots;

    RwLock rwlock;

    public ObjectBlotterDbImpl(){
        version = new AtomicLong(-1L);

        objects = new LinkedList<Pair<Long, V>>();
        snapshots = new ConcurrentHashMap<Long, Long>();

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
    public void putSnapshot(Long tid, Long v) {
        snapshots.put(tid, v);
    }

    @Override
    public void removeSnapshot(Long tid) {
        snapshots.remove(tid);
    }

    public Long getVersionForTransaction(Long tid){
        return snapshots.get(tid);
    }

    public V getValueVersion(long version, Set<Long> aggrDataTx) {
        if (version > getLastVersion())
            return null;

        V value = null;

        for(Pair<Long, V> pair : objects){
            if(pair.f <= version){
                value = pair.s;
                break;
            }
        }

        // Add tids to transaction metadata
        for (Long tid : snapshots.keySet()) {
            Long v = snapshots.get(tid);
            if (v != null && v < version)
                aggrDataTx.add(tid);
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
        objects.addFirst(new Pair(version.incrementAndGet(), value));
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
        return "ObjectBlotterDbImpl@" + hashCode()+" {" +
                "objects=" + objects +
                ", snapshots=" + snapshots +
                '}';
    }
}