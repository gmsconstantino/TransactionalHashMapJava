package databaseOCC;

import database.ObjectDb;

/**
 * Created by gomes on 10/03/15.
 */
public interface ObjectVersionDB<K,V> extends ObjectDb<K,V> {

    public long getVersion();

}
