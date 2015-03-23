package databaseOCC;

import database2PL.Config;
import database.*;

import java.util.*;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K,V> extends database.Transaction<K,V> {

    protected Map<K, ObjectVersionDB<K,V>> readSet; //set , conf se add nao altera
    protected Map<K, ObjectVersionDB<K,V>> writeSet;

    public Transaction(Database db) {
        super(db);
    }

    protected synchronized void init(){
        super.init();

        readSet = new HashMap<K, ObjectVersionDB<K,V>>();
        writeSet = new HashMap<K, ObjectVersionDB<K,V>>();
    }

    @Override
    public V get(K key) throws TransactionTimeoutException, TransactionAbortException {
        if (!isActive)
            return null;

        if(writeSet.containsKey(key)){
            return (V) writeSet.get(key).getValue();
        }

        ObjectVersionDB<K,V> obj = (ObjectVersionDB) getKeyDatabase(key);
        if (obj == null || obj.getVersion() == -1)
            return null;

        if(obj.try_lock_read_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){

            if (readSet.containsKey(key)){
                if (obj.getVersion() == readSet.get(key).getVersion())
                    return obj.getValue();
                else {
                    abort();
                    throw new TransactionAbortException("Transaction Abort " + getId() +": Thread "+Thread.currentThread().getName()+" - Version change - key:"+key);
                }
            } else {
                addObjectDbToReadBuffer((K) key, new BufferObjectVersionDB(key, obj.getValue(), obj.getVersion(), obj, false)); // isNew = false
                V value = (V) obj.getValue();
                obj.unlock_read();
                return value;
            }
        } else {
            abort();
            throw new TransactionTimeoutException("Transaction " + getId() +": Thread "+Thread.currentThread().getName()+" - get key:"+key);
        }
    }

    @Override
    public V get_to_update(K key) throws TransactionTimeoutException{
        return get(key);
    }

    @Override
    public void put(K key, V value) throws TransactionTimeoutException{
        if(!isActive)
            return;

        if(writeSet.containsKey(key)){
            ObjectDb<K,V> objectDb = writeSet.get(key);
            objectDb.setValue(value);
            return;
        }

        ObjectVersionDB<K,V> obj = (ObjectVersionDB) getKeyDatabase(key);
        if (obj == null) {
            obj = new ObjectVersionDBImpl<K,V>(null); // A thread fica com o write lock
            ObjectVersionDB<K,V> objdb = (ObjectVersionDB) putIfAbsent(key, obj);
            obj.unlock_write();

            if (objdb != null)
                obj = objdb;

            ObjectVersionDB<K,V> buffer = new BufferObjectVersionDB(key, obj.getValue(), obj.getVersion(), obj, true); // isNew = true
            buffer.setValue(value);
            addObjectDbToWriteBuffer(key, buffer);
            return;
        }

        // o objecto esta na base de dados
        if(obj.try_lock_read_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){
            ObjectVersionDB<K,V> buffer = new BufferObjectVersionDB(key, obj.getValue(), obj.getVersion(), obj, false);
            buffer.setValue(value);
            addObjectDbToWriteBuffer(key, buffer);
            obj.unlock_read();
        } else {
            abort();
            throw new TransactionTimeoutException("Transaction " + getId() +": Thread "+Thread.currentThread().getName()+" - get key:"+key);
        }
    }

    @Override
    public boolean commit() throws TransactionTimeoutException, TransactionAbortException {
        if(!isActive)
            return success;

        Set<ObjectDb<K,V>> lockObjects = new HashSet<>();

        for (ObjectVersionDB<K,V> buffer : writeSet.values()){
            ObjectVersionDB<K,V> objectDb = (ObjectVersionDB) buffer.getObjectDb();
            if(objectDb.try_lock_write_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){
                lockObjects.add(objectDb);

                if (buffer.getVersion() == objectDb.getVersion()) {
                    continue;
                } else {
                    abortVersions(lockObjects);
                    return false;
                }
            } else {
                abortTimeout(lockObjects);
                return false;
            }
        }

        // Validate Read Set
        for (ObjectVersionDB<K,V> buffer : readSet.values()){ // BufferObject
            ObjectVersionDB<K,V> objectDb = (ObjectVersionDB) buffer.getObjectDb();
            if(objectDb.try_lock_read_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){

                if (buffer.getVersion() == objectDb.getVersion()) {
                    objectDb.unlock_read();
                    continue;
                } else {
                    abortVersions(lockObjects);
                    return false;
                }
            } else {
                abortTimeout(lockObjects);
            }
        }

        // Escrita
        for (ObjectDb<K,V> buffer : writeSet.values()){
            ObjectDb<K,V> objectDb = buffer.getObjectDb();
            if (buffer.isNew()) {
                objectDb.setOld();
            }
            objectDb.setValue(buffer.getValue());
        }

        commitId = Database.timestamp.getAndIncrement();
        unlockWrite_objects(lockObjects);

        isActive = false;
        success = true;
        return true;
    }

    private void abortTimeout(Set<ObjectDb<K,V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionTimeoutException("Transaction " + getId() +": Thread "+Thread.currentThread().getName()+" - commit");
    }

    private void abortVersions(Set<ObjectDb<K,V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionAbortException("Transaction Abort " + getId() +": Thread "+Thread.currentThread().getName()+" - Version change");
    }

    private void unlockWrite_objects(Set<ObjectDb<K,V>> set){
        Iterator<ObjectDb<K,V>> it_locks = set.iterator();
        while (it_locks.hasNext()) {
            ObjectDb<K,V> objectDb = it_locks.next();
            objectDb.unlock_write();
        }
    }

    @Override
    public void abort() throws TransactionAbortException{
        isActive = false;
        success = false;
        commitId = -1;
    }

    void addObjectDbToReadBuffer(K key, ObjectVersionDB<K,V> objectDb){
        readSet.put(key, objectDb);
    }

    void addObjectDbToWriteBuffer(K key, ObjectVersionDB objectDb){
        writeSet.put(key, objectDb);
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
