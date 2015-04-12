package databaseBlotter;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import database.ObjectDb;
import database2PL.Config;
import databaseOCC.ObjectVersionLockDB;
import databaseOCC.ObjectVersionLockDBImpl;
import databaseOCCMulti.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import structures.RwLock;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Set;
import java.util.UUID;
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
//    public Cache<Long, Long> snapshots;

    RwLock rwlock;

    public ObjectBlotterDbImpl(){
        version = new AtomicLong(-1L);

        objects = new LinkedList<Pair<Long, ObjectVersionLockDB<K,V>>>();
        snapshots = new ConcurrentHashMap<Long, Long>();
//        snapshots = CacheBuilder.newBuilder()
//                .concurrencyLevel(16)
//                .weakKeys()
//                .removalListener(new RemovalListener<Long, Long>() {
//                    @Override
//                    public void onRemoval(RemovalNotification<Long, Long> removalNotification) {
//                        System.out.println("Remove key: " + removalNotification.getKey() + " " + removalNotification.getValue());
//                        logger.info("Remove key: " + removalNotification.getKey());
//                    }
//                })
//                .build(); // tid -> version

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

    public Long getVersionForTransaction(Long tid){
        return snapshots.get(tid);
    }

    public V getValueVersion(long version, Set<Long> aggrDataTx) {
        if (version > getLastVersion())
            return null;

        V value = null;

        for(Pair<Long, ObjectVersionLockDB<K,V>> pair : objects){
            if(pair.f <= version){
                value = pair.s.getValue();
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
        ObjectVersionLockDB<K,V> obj = new ObjectVersionLockDBImpl<K,V>(value);
        obj.unlock_write();
        objects.addFirst(new Pair(version.incrementAndGet(), obj));
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