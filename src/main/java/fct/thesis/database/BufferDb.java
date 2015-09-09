package fct.thesis.database;

/**
 * Created by gomes on 23/04/15.
 */
public interface BufferDb<K,V> extends ObjectDb<V> {

    public K getKey();
    public int getTable();
    public void setVersion(long version);
}
