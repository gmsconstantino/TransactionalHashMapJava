package databaseOCC;

import database.ObjectLockDb;

/**
 * Created by gomes on 10/03/15.
 */
public interface ObjectVersionLockDB<K,V> extends ObjectLockDb<K,V> {

    public long getVersion();

}
