package database2PL;

import database.*;

import java.util.*;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K,V> extends database.Transaction<K,V> {

    protected Set<K> readSet;

    // Write set guarda o valor antigo que estava antes da transacao
    protected Map<K, ObjectDb<K,V>> writeSet;

    public Transaction(Database db) {
        super(db);
    }

    protected void init(){
        super.init();

        readSet = new HashSet<K>();
        writeSet = new HashMap<>();
    }

    @Override
    public V get(K key) {
        if (!isActive)
            return null;

        if (readSet.contains(key)){
            return (V) getKeyDatabase(key).getObjectDb().getValue();
        }

        // Passa a ser um objecto do tipo ObjectDbImpl
        ObjectDb<?,?> obj = getKeyDatabase(key);

        if (obj == null)
            return null;

        if(obj.try_lock_read_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){
            addObjectDbToReadBuffer((K) key);
            return (V) obj.getValue();
        } else {
            abort();
            throw new TransactionTimeoutException("Transaction " + getId() +": Thread "+Thread.currentThread().getName()+" - get key:"+key);
        }
    }

    @Override
    public V get_to_update(K key) {
        if (!isActive)
            return null;

        ObjectDb<K,V> obj = getObjectFromWriteBuffer(key);
        if (obj != null){
            return obj.getValue();
        }

        // Passa a ser um objecto do tipo ObjectDbImpl
        obj = getKeyDatabase(key);

        if (obj == null)
            return null;

        if(obj.try_lock_write_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){
            addObjectDbToWriteBuffer(key, new BufferObjectDb(obj.getValue(), obj));
            return obj.getValue();
        } else {
            abort();
            throw new TransactionTimeoutException("Transaction " + getId() +": Thread "+Thread.currentThread().getName()+" - get key:"+key);
        }
    }

    @Override
    public void put(K key, V value) {
        if (!isActive)
            return;

        // Se esta na cache é pq ja tenho o write lock do objecto
        ObjectDb<K,V> obj = getObjectFromWriteBuffer(key);
        if (obj != null){
            obj.setValue(value);
            return;
        }

        // Search
        obj = getKeyDatabase(key); // Passa a ser um objecto do tipo ObjectDbImpl

        if(obj == null){
            obj = new ObjectDbImpl<K,V>(value); // A thread fica com o write lock

            ObjectDb<K,V> map_obj = putIfAbsent(key, obj);
            obj = map_obj!=null? map_obj : obj;

            addObjectDbToWriteBuffer(key, obj);

            return;
        }

        if(obj.try_lock_write_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){
            addObjectDbToWriteBuffer(key, new BufferObjectDb<K,V>(obj.getValue(), obj));
            obj.setValue(value);
        } else {
            abort();
            throw new TransactionTimeoutException("Transaction " + getId() +": Thread "+Thread.currentThread().getName()+" - put key:"+key+" value:"+value);
        }
    }

    @Override
    public boolean commit() {
        if (!isActive)
            return success;

        commitId = Database.timestamp.getAndIncrement();

        Set<Map.Entry<K,ObjectDb<K,V>>> entrySet =  writeSet.entrySet();
        for (Map.Entry<K,ObjectDb<K,V>> obj : entrySet){
            ObjectDb objectDb = obj.getValue();
            objectDb.setOld();
        }

        unlock_locks();
        isActive = false;
        success = true;

        return true;
    }

    @Override
    public void abort() {
        // Cleanup
        Set<Map.Entry<K,ObjectDb<K,V>>> entrySet =  writeSet.entrySet();
        for (Map.Entry<K,ObjectDb<K,V>> obj : entrySet){
            ObjectDb objectDb = obj.getValue();
            if (objectDb.isNew()){
                removeKey(obj.getKey());
                readSet.remove(obj.getKey());
            } else {
                objectDb.getObjectDb().setValue(objectDb.getValue());
            }
        }

        unlock_locks();
        isActive = false;
        success = false;
    }

    private void unlock_locks(){
        Iterator<K> it_keys = readSet.iterator();
        while (it_keys.hasNext()) {
            K key = it_keys.next();
            ObjectDb objectDb = getKeyDatabase(key);
            objectDb.unlock_read();
        }

        for (ObjectDb objectDb : writeSet.values()) {
            objectDb.unlock_write();
        }
    }

    void addObjectDbToReadBuffer(K key){
        readSet.add(key);
    }

    void addObjectDbToWriteBuffer(K key, ObjectDb obj){
        writeSet.put(key, obj);
    }

    ObjectDb<K,V> getObjectFromWriteBuffer(K key){
        return (writeSet.get(key)!=null) ? writeSet.get(key).getObjectDb() : null;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "id=" + id +
                ", thread=" + Thread.currentThread().getName() +
                ", readSet=" + readSet +
                ", writeSet=" + writeSet +
                ", isActive=" + isActive +
                ", success=" + success +
                '}';
    }

}
