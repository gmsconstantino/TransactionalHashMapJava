package fct.thesis.databaseBlotter;

import fct.thesis.database.ObjectDb;
import fct.thesis.databaseOCCMulti.Pair;

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
    void removeSnapshot(Long id);

    public void lock_read();
    public void lock_write();

    public boolean try_lock_write_for(long time, TimeUnit unit);
    public boolean try_lock_read_for(long time, TimeUnit unit);
}
