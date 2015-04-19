package fct.thesis.databaseOCC2;

import fct.thesis.database.ObjectLockDb;

/**
 * Created by gomes on 10/03/15.
 */
public interface ObjectOCC2<K,V> extends ObjectLockDb<K,V> {

    public long getVersion();

    public void lock_read();

}
