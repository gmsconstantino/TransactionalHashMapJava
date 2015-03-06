package database;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by gomes on 28/02/15.
 */
public interface ObjectDb<K,V> {

    static AtomicLong timestamp = new AtomicLong(0);

    public V getValue();
    public K getKey();
    public long getVersion();
    public void setValue(V value);

    public ObjectDb getObjectDb();

    public boolean try_lock_write_for(long time, TimeUnit unit);
    public boolean try_lock_read_for(long time, TimeUnit unit);

    public void unlock_read();
    public void unlock_write();

    public boolean isNew();
    public void setOld();
}
