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
    public boolean isNew;

    public RWEntry(MyObject<?, V> obj, long version, K key, V newValue, boolean isNew) {
        this.obj = obj;
        this.version = version;
        this.key = key;
        this.newValue = newValue;
        this.isNew = isNew;
    }


}
