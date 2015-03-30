package databaseBlotter;

import database.ObjectDb;
import databaseOCCMulti.Pair;

import java.util.List;
import java.util.Set;

/**
 * Created by gomes on 29/03/15.
 */
public interface ObjectBlotterDb<K,V> extends ObjectDb<K,V> {

    public Pair<V,List<Long>> getValueTransaction(long id);

    public boolean preWrite(long id, V value);

    public void unPreWrite();

    public void write(Set<Long> agg);

    public Long getLastVersion();

}
