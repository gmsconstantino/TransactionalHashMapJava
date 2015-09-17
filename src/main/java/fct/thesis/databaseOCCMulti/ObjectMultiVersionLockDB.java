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

//    LinkedList<P<Long, V>> objects;
    RwLock lock;

    protected class VTuple {
        public long version;
        public V object;
        public VTuple next;

        public VTuple(long version, V object, VTuple next) {
            this.version = version;
            this.object = object;
            this.next = next;
        }
    }

    public ObjectMultiVersionLockDB(){
        super();
        lock = new RwLock();
    }

    protected VTuple lastVersion = null;

    public long getVersion() {
        if(lastVersion == null)
            return -1L;
        return lastVersion.version;
    }

    public V getValueVersionLessOrEqual(long localClock) {
        VTuple curr = lastVersion;
        while(curr != null) {
            if (curr.version <= localClock)
                return curr.object;
            curr = curr.next;
        }
        return null;
    }

    public void addNewVersion(long version, V object) {
        VTuple newVersion = new VTuple(version, object, lastVersion);
        lastVersion = newVersion;
    }

    public void clean(int localClock) {
        VTuple curr = lastVersion;
        lock_write();
        while(curr != null) {
            if (curr.version <= localClock){
                curr.next = null;
            }
            curr = curr.next;
        }
        unlock_write();
    }


//    @Override
//    public V getValue() {
//        if(objects.isEmpty())
//            return null;
//        return objects.getFirst().s;
//    }

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