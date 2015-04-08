package databaseBlotter;

import database.ObjectDb;
import databaseOCCMulti.Pair;

import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 29/03/15.
 */
public interface ObjectBlotterDb<K,V> extends ObjectDb<K,V> {

    public Long getVersionForTransaction(long id);

    public Pair<V,ListIterator<Long>> getValueVersion(long version);

    public void setValue(V value);

    public Long getLastVersion();

    public Long incrementAndGetVersion();

    public void lock_read();
    public void lock_write();

    public boolean try_lock_write_for(long time, TimeUnit unit);
    public boolean try_lock_read_for(long time, TimeUnit unit);

    public void unlock_read();
    public void unlock_write();

    void putSnapshot(long id, Long v);
}
