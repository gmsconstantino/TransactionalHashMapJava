package databaseBlotter;

import database.ObjectDb;
import databaseOCCMulti.Pair;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 29/03/15.
 */
public interface ObjectBlotterDb<K,V> extends ObjectDb<K,V> {

    public Long getVersionForTransaction(long id);

    public Pair<V,List<Long>> getValueVersion(long version);

    public void setValue(V value);

    public Long getLastVersion();

    public Long incrementAndGetVersion();

    public void lock();

    public boolean tryLock(long timeout, TimeUnit unit);

    public void unlock();

    void putSnapshot(long id, Long v);
}
