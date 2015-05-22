package fct.thesis.database;

/**
 * Created by gomes on 23/04/15.
 */
public interface BufferDb<K,V> extends ObjectDb<K,V> {

    public K getKey();
    public int getTable();
    public long getVersion();
    public void setVersion(long version);
}
