package fct.thesis.database;

/**
 * Created by gomes on 30/03/15.
 */
public interface ObjectDb<V> {

    public ObjectDb getObjectDb();

    public V getValue();

    public void setValue(V value);

    public long getVersion();

    public void clean(long version);

}
