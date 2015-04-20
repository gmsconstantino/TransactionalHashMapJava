package fct.thesis.databaseOCC2;

import fct.thesis.database.ObjectDb;
import fct.thesis.database.ObjectLockDb;
import fct.thesis.database.TransactionAbortException;
import pt.dct.util.P;

import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 10/03/15.
 */
public interface ObjectOCC2<K,V> extends ObjectDb<K,V> {

    public V getValue();

    public void setValue(V value);

    public long getVersion();

    public ObjectDb<K,V> getObjectDb();

    public V readVersion(long version) throws TransactionAbortException;

    public P<V,Long> readLast() throws TransactionAbortException;

    public boolean try_lock(long time, TimeUnit unit);

    public void unlock();

}
