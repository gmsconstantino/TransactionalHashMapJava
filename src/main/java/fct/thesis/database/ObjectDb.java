package fct.thesis.database;

/**
 * Created by gomes on 30/03/15.
 */
public interface ObjectDb<K,V> {

    public ObjectDb getObjectDb();

    public V getValue();

    public void setValue(V value);

    public void clean(long version);

}
