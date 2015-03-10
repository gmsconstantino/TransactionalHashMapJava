package database;

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
        id = Database.timestamp.getAndIncrement();
        isActive = true;
        success = false;
    }

    public abstract V get(K key) throws TransactionTimeoutException;

    public abstract V get_to_update(K key) throws TransactionTimeoutException;

    public abstract void put(K key, V value) throws TransactionTimeoutException;

    public abstract boolean commit() throws TransactionTimeoutException;

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