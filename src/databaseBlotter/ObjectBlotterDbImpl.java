package databaseBlotter;

import database.ObjectDb;
import databaseOCC.ObjectVersionDB;
import databaseOCC.ObjectVersionDBImpl;
import databaseOCCMulti.Pair;
import structures.RwLock;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by gomes on 23/03/15.
 */
public class ObjectBlotterDbImpl<K,V> implements ObjectBlotterDb<K,V>{

    public AtomicInteger prewrite;

    AtomicLong version;
    long last_version;

    LinkedList<Pair<Long, ObjectVersionDB<K,V>>> objects;
    ConcurrentHashMap<Long, Long> snapshots;

    RwLock lock;


    public ObjectBlotterDbImpl(){
        version = new AtomicLong(0);
        prewrite = new AtomicInteger(0);

        objects = new LinkedList<>();
        snapshots = new ConcurrentHashMap<>();

        lock = new RwLock();
        last_version = -1;
        lock.lock_write();
    }

    public long getVersion() {
        return last_version;
    }

    public V getValueTransaction(long id) {
        Long version = snapshots.get(id);

        if (version==null){
            while (prewrite.get()==1){}

            snapshots.put(id,last_version);
            version = last_version;
        }

        for(Pair<Long, ObjectVersionDB<K,V>> pair : objects){
            if(pair.f <= version){
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