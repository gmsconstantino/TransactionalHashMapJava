package fct.thesis.databaseOCCMulti;

import fct.thesis.database.ObjectDb;
import fct.thesis.database.ObjectLockDb;
import fct.thesis.database.ObjectLockDbAbstract;
import fct.thesis.structures.RwLock;
import pt.dct.util.P;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 23/03/15.
 */
public class ObjectMultiVersionLockDB<K extends Comparable<K>,V> extends ObjectLockDbAbstract<V> {

    LinkedList<P<Long, V>> objects;
    RwLock lock;

    public ObjectMultiVersionLockDB(){
        super();
        objects = new LinkedList<>();
        lock = new RwLock();
    }

    public long getVersion() {
        return objects.getFirst().f;
    }

    public V getValueVersionLessOrEqual(long timestamp) {
        for(P<Long, V> pair : objects){
            if(pair.f <= timestamp){
                return pair.s;
            }
        }
        return null;
    }

    public void addNewVersion(Long version, V value){
//        BufferObjectDb<K,V> obj = new BufferObjectDb<K,V>(value);
        objects.addFirst(new P(version, value));
    }

    @Override
    public V getValue() {
        if(objects.isEmpty())
            return null;
        return objects.getFirst().s;
    }

    @Override
    public void clean(long version) {
        if (objects.size()==1)
            return;

        lock_write();
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
        unlock_write();
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
        return "ObjectMultiVersionLockDB{}";
    }

    public static void main(String[] args) {
        ObjectMultiVersionLockDB<Integer,Integer> obj = new ObjectMultiVersionLockDB<>();
        obj.addNewVersion(0L, 1);
        obj.addNewVersion(3L, 2);
        obj.addNewVersion(4L, 3);
        obj.addNewVersion(7L, 4);

        System.out.println(obj.getValueVersionLessOrEqual(6L));
    }
}