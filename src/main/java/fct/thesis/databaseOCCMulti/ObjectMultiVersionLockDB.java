package fct.thesis.databaseOCCMulti;

import fct.thesis.database.BufferObjectDb;
import fct.thesis.database.ObjectDb;
import fct.thesis.database.ObjectLockDb;
import fct.thesis.database.ObjectLockDbAbstract;
import fct.thesis.databaseOCC.ObjectLockOCC;
import fct.thesis.structures.RwLock;
import pt.dct.util.P;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 23/03/15.
 */
public class ObjectMultiVersionLockDB<K extends Comparable<K>,V> extends ObjectLockDbAbstract<K,V> {

    volatile long last_version;
    ConcurrentLinkedDeque<P<Long, ObjectDb<K,V>>> objects;
    RwLock lock;

    public ObjectMultiVersionLockDB(){
        super();
        objects = new ConcurrentLinkedDeque<>();
        lock = new RwLock();
        last_version = -1;
    }

    public long getVersion() {
        return last_version;
    }

    public V getValueVersionLess(long startTime) {
        for(P<Long, ObjectDb<K,V>> pair : objects){
            if(pair.f <= startTime){
                return pair.s.getValue();
            }
        }
        return null;
    }

    public void addNewVersionObject(Long version, V value){
        BufferObjectDb<K,V> obj = new BufferObjectDb<K,V>(value);
        objects.addFirst(new P(version, obj));
        last_version = version;
    }

    @Override
    public V getValue() {
        if(last_version == -1)
            return null;
        return objects.getFirst().s.getValue();
    }

    @Override
    public void clean(long version) {
        if (objects.size()==1)
            return;

        Iterator<P<Long, ObjectDb<K,V>>> it = objects.descendingIterator();
        while (it.hasNext()){
            P<Long, ObjectDb<K,V>> pair = it.next();
            if (version >= pair.f)
                objects.removeLastOccurrence(pair);
            else
                break;

            if (objects.size()==1)
                return;
        }
    }

    @Override
    public void setValue(V value) {

    }

    @Override
    public ObjectLockDb getObjectDb() {
        return this;
    }

    @Override
    public boolean try_lock_write_for(long time, TimeUnit unit) {
        return lock.try_lock_write_for(time, unit);
    }

    @Override
    public boolean try_lock_read_for(long time, TimeUnit unit) {
        return lock.try_lock_read_for(time,unit);
    }

    @Override
    public void unlock_read() {
        lock.unlock_read();
    }

    @Override
    public void unlock_write() {
        lock.unlock_write();
    }

    @Override
    public void lock_write() {
        lock.lock_write();
    }

    @Override
    public void lock_read() {
        lock.lock_read();
    }

    @Override
    public String toString() {
        return "ObjectMultiVersionLockDB{" +
                "last_version=" + last_version +
                ", lock=" + lock +
                '}';
    }
}