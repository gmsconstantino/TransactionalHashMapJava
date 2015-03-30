package databaseBlotter;

import databaseOCC.ObjectVersionDB;

/**
 * Created by gomes on 29/03/15.
 */
public interface ObjectBlotterDb<K,V> extends ObjectVersionDB<K,V> {

    public V getValueTransaction(long id);

}
