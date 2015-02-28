package database;

import java.util.concurrent.TimeUnit;

/**
 * Created by gomes on 28/02/15.
 */
public interface ObjectDb<V> {

    public V getValue();
    public void setValue(V value);

    public boolean try_lock_write_for(long time, TimeUnit unit);
    public boolean try_lock_read_for(long time, TimeUnit unit);

    public void unlock_read();
    public void unlock_write();
}
