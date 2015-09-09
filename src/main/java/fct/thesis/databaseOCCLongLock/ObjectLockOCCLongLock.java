package fct.thesis.databaseOCCLongLock;

import fct.thesis.database.ObjectLockDb;
import fct.thesis.database.ObjectLockDbAbstract;
import fct.thesis.structures.RwLock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by gomes on 26/02/15.
 */
public class ObjectLockOCCLongLock<K,V> extends ObjectLockDbAbstract<V> {

    private AtomicLong version;

    public ObjectLockOCCLongLock(V value){
        super(value);
        version = new AtomicLong();
    }

    public boolean compareAndSet(long expected, long update){
        return version.compareAndSet(expected, update);
    }

    @Override
    public String toString() {
        return "ObjectLockOCCLongLock{" +
                "value=" + getValue() +
                ", version=" + version +
                '}';
    }

    @Override
    public long getVersion() {
        return version.get();
    }

    @Override
    public void setValue(V value) {
        super.setValue(value);
    }

    @Override
    public ObjectLockDb getObjectDb() {
        return this;
    }
}
