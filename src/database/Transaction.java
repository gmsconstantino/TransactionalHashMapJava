package database;

import java.util.*;
import java.util.concurrent.locks.Lock;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K,V> implements Comparable {

    static long count = 0;


    long id;
    long commit_id;
    Map<K,ObjectDb<K,V>> lockList;

    public Transaction(){
        init();
    }

    private synchronized void init(){
        id = count++;
        lockList = new HashMap<K, ObjectDb<K, V>>();
    }

    void addObjectDb(K key, ObjectDb objectDb){
        lockList.put(key, objectDb);
    }

    ObjectDb<K,V> getObjectDb(K key){
        return lockList.get(key);
    }

    public synchronized void commit(){
        unlock_locks();
        commit_id = Database.commit_count++;
    }

    public void abort(){

        unlock_locks();

    }

    private void unlock_locks(){
        for(ObjectDb objectDb : lockList.values()){
            objectDb.unlock();
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
}
