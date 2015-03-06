package database;

/**
 * Created by gomes on 26/02/15.
 */

public abstract class Transaction<K,V> implements Comparable {

    long id;
    long commitId;
    public boolean isActive;
    public boolean success;

    protected Database db;

    public Transaction(Database db){
        this.db = db;
        init();
    }

    protected void init(){
        id = Database.timestamp.getAndIncrement();
        isActive = true;
        success = false;
    }

    public abstract V get(K key) throws TransactionTimeoutException;

    public abstract V get_to_update(K key) throws TransactionTimeoutException;

    public abstract void put(K key, V value) throws TransactionTimeoutException;

    public abstract boolean commit() throws TransactionTimeoutException;

    public abstract void abort() throws TransactionTimeoutException;

    @Override
    public int compareTo(Object o) {
        Transaction t = (Transaction) o;
        return Long.compare(this.id, t.id);
    }

    public long getId() {
        return id;
    }

    public long getCommitId() {
        return commitId;
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