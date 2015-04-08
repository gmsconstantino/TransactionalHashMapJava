package databaseOCCMulti;

import database.ObjectLockDb;
import databaseOCC.ObjectVersionLockDB;
import databaseOCC.ObjectVersionLockDBImpl;
import structures.RwLock;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 23/03/15.
 */
public class ObjectMultiVersionLockDB<K,V> implements ObjectVersionLockDB<K,V> {

    long last_version;
    LinkedList<Pair<Long, ObjectVersionLockDB<K,V>>> objects;
    RwLock lock;


    public ObjectMultiVersionLockDB(){
        objects = new LinkedList<Pair<Long, ObjectVersionLockDB<K,V>>>();
        lock = new RwLock();
        last_version = -1;
        lock.lock_write();
    }

    public long getVersion() {
        return last_version;
    }

    public V getValueVersionLess(long startTime) {
        for(Pair<Long, ObjectVersionLockDB<K,V>> pair : objects){
            if(pair.f <= startTime){
                return pair.s.getValue();
            }
        }
        return null;
    }

    public void addNewVersionObject(Long version, V value){
        last_version = version;
        ObjectVersionLockDBImpl<K,V> obj = new ObjectVersionLockDBImpl<K,V>(value);
        objects.addFirst(new Pair(version, obj));
        obj.unlock_write();
    }

    @Override
    public V getValue() {
        if(last_version == -1)
            return null;
        return objects.getFirst().s.getValue();
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
    public boolean isNew() {
        return false;
    }

    @Override
    public void setOld() {

    }

    @Override
    public String toString() {
        return "ObjectMultiVersionDB{" +
                "last_version=" + last_version +
                ", objects=" + objects +
                '}';
    }

}