package databaseBlotter;

import database.ObjectDb;
import databaseOCCMulti.Pair;

import java.lang.ref.WeakReference;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 29/03/15.
 */
public interface ObjectBlotterDb<K,V> extends ObjectDb<K,V> {

    public Long getVersionForTransaction(Long id);

    public V getValueVersion(long version, Set<Long> aggrDataTx);

    public void setValue(V value);

    public Long getLastVersion();

    public Long incrementAndGetVersion();

    public void unlock_read();
    public void unlock_write();

    void putSnapshot(Long id, Long v);

    public void lock_read();
    public void lock_write();

    public boolean try_lock_write_for(long time, TimeUnit unit);
    public boolean try_lock_read_for(long time, TimeUnit unit);
}
