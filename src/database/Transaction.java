package database;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K> implements Comparable {

    public static AtomicLong commit_count = new AtomicLong(0);
    static AtomicLong count = new AtomicLong(0);

    long id;
    long commit_id;

    Map<K,ObjectDb<?,?>> readSet;
    Map<K,ObjectDb<?,?>> writeSet;

    public boolean isActive;
    public boolean success;

    public Transaction(){
        init();
    }

    private synchronized void init(){
        id = count.getAndIncrement();

        isActive = true;
        success = false;

        readSet = new HashMap<K, ObjectDb<?, ?>>();
        writeSet = new HashMap<K, ObjectDb<?, ?>>();
    }

    void add_Read_ObjectDb(K key, ObjectDb objectDb){
        readSet.put(key, objectDb);
    }

    void add_Write_ObjectDb(K key, ObjectDb objectDb){
        writeSet.put(key, objectDb);
    }

    ObjectDb<?,?> getObjectDb(K key){
        ObjectDb<?,?> obj = readSet.get(key);
        if(obj != null){
            return obj;
        } else
            return writeSet.get(key);
    }

    public synchronized void commit(){
        commit_id = commit_count.getAndIncrement();

        //TODO: tirar do buffer e actualizar os objetos

        unlock_locks();
        isActive = false;
        success = true;
    }

    public void abort(){
        unlock_locks();
        isActive = false;
        commit_id = -1;
    }

    private void unlock_locks(){
        for(ObjectDb objectDb : readSet.values()){
            objectDb.unlock_read();
        }
        for(ObjectDb objectDb : writeSet.values()){
            objectDb.unlock_write();
        }
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
