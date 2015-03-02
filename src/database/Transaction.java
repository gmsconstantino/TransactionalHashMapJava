package database;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K> implements Comparable {

    public static AtomicLong commit_count = new AtomicLong(0);
    static AtomicLong count = new AtomicLong(0);

    long id;
    long commit_id;

    protected Map<K, ObjectDb<?>> readSet;
    protected Map<K, ObjectDb<?>> writeSet;

    public boolean isActive;
    public boolean success;

    public Transaction(){
        init();
    }

    private synchronized void init(){
        id = count.getAndIncrement();

        isActive = true;
        success = false;

        readSet = new HashMap<K, ObjectDb<?>>();
        writeSet = new HashMap<K, ObjectDb<?>>();
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
    public int compareTo(Object o) {
        Transaction t = (Transaction) o;
        return Long.compare(this.id, t.id);
    }

    public long getId() {
        return id;
    }

    public long getCommit_id() {
        return commit_id;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "id=" + id +
                ", commit_id=" + commit_id +
                ", readSet=" + readSet +
                ", writeSet=" + writeSet +
                ", isActive=" + isActive +
                ", success=" + success +
                '}';
    }
}
