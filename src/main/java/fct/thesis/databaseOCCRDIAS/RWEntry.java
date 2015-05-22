package fct.thesis.databaseOCCRDIAS;

/**
 * @author Ricardo Dias
 * @date 04/05/15
 */
public class RWEntry<K, V> {
    public MyObject<?,V> obj;
    public long version;
    public K key;
    public V newValue;
    public int table;
    public boolean isNew;

    public RWEntry(int table, MyObject<?, V> obj, long version, K key, V newValue, boolean isNew) {
        this.table = table;
        this.obj = obj;
        this.version = version;
        this.key = key;
        this.newValue = newValue;
        this.isNew = isNew;
    }


}
