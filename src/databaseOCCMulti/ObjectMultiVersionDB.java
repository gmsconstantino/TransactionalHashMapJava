package databaseOCCMulti;

import database.ObjectDb;
import database2PL.Config;
import databaseOCC.ObjectVersionDB;
import databaseOCC.ObjectVersionDBImpl;
import structures.RwLock;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by gomes on 23/03/15.
 */
public class ObjectMultiVersionDB<K,V> implements ObjectVersionDB<K,V>{

    long last_version;
    LinkedList<Pair<Long, ObjectVersionDB<K,V>>> objects;
    RwLock lock;


    ObjectMultiVersionDB(){
        objects = new LinkedList<>();
        lock = new RwLock();
        last_version = -1;
        lock.lock_write();
    }

    public long getVersion() {
        return last_version;
    }

    public V getValueVersionLess(long startTime) {
        for(Pair<Long, ObjectVersionDB<K,V>> pair : objects){
            if(pair.f <= startTime){
                return pair.s.getValue();
            }
        }
        return null;
    }

    public void addNewVersionObject(Long version, V value){
        last_version = version;
        ObjectVersionDBImpl<K,V> obj = new ObjectVersionDBImpl<K,V>(value);
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
    public ObjectDb getObjectDb() {
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