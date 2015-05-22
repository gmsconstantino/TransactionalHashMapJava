package fct.thesis.database;

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

    public abstract V get(int table, K key) throws TransactionTimeoutException, TransactionAbortException;

    public abstract void put(int table, K key, V value) throws TransactionTimeoutException, TransactionAbortException;

    public abstract boolean commit() throws TransactionTimeoutException, TransactionAbortException;

    public abstract void abort() throws TransactionTimeoutException;

    protected ObjectDb<K,V> getKeyDatabase(int table, K key){
        return db.getKey(table, key);
    }

    protected ObjectDb<K,V> putIfAbsent(int table, K key, ObjectDb<K,V> obj){
        return db.putIfAbsent(table, key, obj);
    }

    protected ObjectDb<K,V> removeKey(int table, K key){
        return db.removeKey(table, key);
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
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
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