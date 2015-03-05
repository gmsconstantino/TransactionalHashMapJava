package database;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by gomes on 26/02/15.
 */

public class TransactionOCC<K,V> extends Transaction<K,V> {

    protected Map<K, ObjectDb<?>> readSet;
    protected Map<K, ObjectDb<?>> writeSet;

    public TransactionOCC(Database db) {
        super(db);
    }

    protected synchronized void init(){
        super.init();

        readSet = new HashMap<K, ObjectDb<?>>();
        writeSet = new HashMap<K, ObjectDb<?>>();
    }

    @Override
    public V get(Object key) {
        return null;
    }

    @Override
    public V get_to_update(K key) {
        return null;
    }

    @Override
    public void put(K key, V value) {

    }

    @Override
    public boolean commit() {
        return false;
    }

    @Override
    public void abort() {
        return;
    }

    void addObjectDbToReadBuffer(K key, ObjectDb objectDb){
        readSet.put(key, objectDb);
    }

    void addObjectDbToWriteBuffer(K key, ObjectDb objectDb){
        writeSet.put(key, objectDb);
    }

    ObjectDb<?> getObjectFromReadBuffer(K key){
        return (readSet.get(key)!=null) ? readSet.get(key).getObjectDb() : null;
    }

    ObjectDb<?> getObjectFromWriteBuffer(K key){
        return (writeSet.get(key)!=null) ? writeSet.get(key).getObjectDb() : null;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "id=" + id +
                ", readSet=" + readSet +
                ", writeSet=" + writeSet +
                ", isActive=" + isActive +
                ", success=" + success +
                '}';
    }

}
