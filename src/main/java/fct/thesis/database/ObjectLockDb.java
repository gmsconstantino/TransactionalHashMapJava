package fct.thesis.database;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by gomes on 28/02/15.
 */
public interface ObjectLockDb<K,V> extends ObjectDb<K,V>{

    public boolean try_lock_write_for(long time, TimeUnit unit);
    public boolean try_lock_read_for(long time, TimeUnit unit);

    public void lock_write();
    public void lock_read();

    public void unlock_read();
    public void unlock_write();

    public long getVersion();

}
