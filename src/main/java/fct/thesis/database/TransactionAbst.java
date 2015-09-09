package fct.thesis.database;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by gomes on 26/02/15.
 */

public abstract class TransactionAbst<K,V> implements Comparable {

    public long id;
    public long commitId;
    public boolean isActive;
    public boolean success;

    public Thread thread;
    public int idxThread;

    protected Database db;

    public TransactionAbst(Database db){
        this.db = db;
        init();
    }

    protected void init(){
        id = -1;
        isActive = true;
        success = false;
    }

    public abstract V get(int table, K key) throws TransactionException;

    public V get(K key) throws TransactionException{
        return get(0, key);
    }

    public abstract void put(int table, K key, V value) throws TransactionException;

    public void put(K key, V value) throws TransactionException {
        put(0, key, value);
    }

    public abstract boolean commit() throws TransactionException;

    public abstract void abort() throws TransactionException;

    protected ObjectDb<V> getKeyDatabase(int table, K key){
        return db.getKey(table, key);
    }

    protected ObjectDb<V> putIfAbsent(int table, K key, ObjectDb<V> obj){
        return db.putIfAbsent(table, key, obj);
    }

    protected ObjectDb<V> removeKey(int table, K key){
        return db.removeKey(table, key);
    }

    @Override
    public int compareTo(Object o) {
        TransactionAbst t = (TransactionAbst) o;
        return (new Long(this.id)).compareTo(t.id);
    }

    public long getId() {
        return id;
    }

    public long getCommitId() {
        return commitId;
    }

    public boolean isActive() {
        return isActive;
    }

    public boolean isSuccess() {
        return success;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransactionAbst)) return false;

        TransactionAbst that = (TransactionAbst) o;

        if (id != that.id) return false;

        return true;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "id=" + id +
                ", isActive=" + isActive +
                ", success=" + success +
                '}';
    }

}