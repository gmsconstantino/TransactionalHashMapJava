package fct.thesis.databaseOCCMulti;

import fct.thesis.database.ObjectLockDb;
import fct.thesis.database.ObjectLockDbAbstract;
import fct.thesis.structures.RwLock;
import pt.dct.util.P;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 23/03/15.
 */
public class ObjectMultiVersionLockDB<K extends Comparable<K>,V> extends ObjectLockDbAbstract<V> {

    volatile long last_version;
    ConcurrentLinkedDeque<P<Long, V>> objects;
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
        for(P<Long, V> pair : objects){
            if(pair.f <= startTime){
                return pair.s;
            }
        }
        return null;
    }

    public void addNewVersionObject(Long version, V value){
//        BufferObjectDb<K,V> obj = new BufferObjectDb<K,V>(value);
        objects.addFirst(new P(version, value));
        last_version = version;
    }

    @Override
    public V getValue() {
        if(last_version == -1)
            return null;
        return objects.getFirst().s;
    }

    @Override
    public void clean(long version) {
        if (objects.size()==1)
            return;

        Iterator<P<Long, V>> it = objects.descendingIterator();
        while (it.hasNext()){
            P<Long, V> pair = it.next();
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

    public static void main(String[] args) {
        ObjectMultiVersionLockDB<Integer,Integer> obj = new ObjectMultiVersionLockDB<>();
        obj.addNewVersionObject(0L,1);
        obj.addNewVersionObject(3L,2);
        obj.addNewVersionObject(4L,3);
        obj.addNewVersionObject(7L,4);

        System.out.println(obj.getValueVersionLess(6L));
    }
}