package fct.thesis.database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by gomes on 26/02/15.
 */

public abstract class Transaction<K,V> implements Comparable {

    public long id;
    public long commitId;
    public boolean isActive;
    public boolean success;

    protected Database db;

    public Transaction(Database db){
        this.db = db;
        init();
    }

    protected void init(){
        id = -1;
        isActive = true;
        success = false;
    }

    public abstract V get(K key) throws TransactionTimeoutException, TransactionAbortException;

    public abstract void put(K key, V value) throws TransactionTimeoutException, TransactionAbortException;

    public abstract boolean commit() throws TransactionTimeoutException, TransactionAbortException;

    public abstract void abort() throws TransactionTimeoutException;

    protected ObjectDb<K,V> getKeyDatabase(K key){
        return db.getKey(key);
    }

    protected ObjectDb<K,V> putIfAbsent(K key, ObjectDb<K,V> obj){
        return db.putIfAbsent(key, obj);
    }

    protected ObjectDb<K,V> removeKey(K key){
        return db.removeKey(key);
    }

    @Override
    public int compareTo(Object o) {
        Transaction t = (Transaction) o;
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
        if (!(o instanceof Transaction)) return false;

        Transaction that = (Transaction) o;

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

    public Collection getWriteSet(){
        return new ArrayList<>();
    }

}