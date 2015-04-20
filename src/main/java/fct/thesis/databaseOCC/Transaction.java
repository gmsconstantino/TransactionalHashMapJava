package fct.thesis.databaseOCC;

import fct.thesis.database.Database;
import fct.thesis.database.ObjectLockDb;
import fct.thesis.database.TransactionAbortException;
import fct.thesis.database.TransactionTimeoutException;
import fct.thesis.database2PL.Config;

import java.util.*;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K,V> extends fct.thesis.database.Transaction<K,V> {

    protected Map<K, ObjectVersionLockDB<K,V>> readSet;
    protected Map<K, ObjectVersionLockDB<K,V>> writeSet;

    public Transaction(Database db) {
        super(db);
    }

    protected void init(){
        super.init();

        readSet = new HashMap<K, ObjectVersionLockDB<K,V>>();
        writeSet = new HashMap<K, ObjectVersionLockDB<K,V>>();
    }

    @Override
    public V get(K key) throws TransactionTimeoutException, TransactionAbortException {
        if (!isActive)
            return null;

        if(writeSet.containsKey(key)){
            return (V) writeSet.get(key).getValue();
        }

        V returnValue;

        ObjectVersionLockDB<K,V> obj = (ObjectVersionLockDB) getKeyDatabase(key);
        if (obj == null || obj.getVersion() == -1)
            return null;

        if(obj.try_lock_read_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){
            if (readSet.containsKey(key)){
                if (obj.getVersion() == readSet.get(key).getVersion())
                    returnValue = obj.getValue();
                else {
                    abort();
                    throw new TransactionAbortException("GET: Transaction Abort " + getId() +": "+Thread.currentThread().getName()+" - Version change - key:"+key);
                }
            } else {
                addObjectDbToReadBuffer(key, new BufferObjectVersionDB(key, obj.getValue(), obj.getVersion(), obj));
                returnValue = obj.getValue();
            }
            obj.unlock_read();
        } else {
            abort();
            throw new TransactionTimeoutException("GET: Transaction " + getId() +": "+Thread.currentThread().getName()+" - get key:"+key);
        }

        return returnValue;
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
            ObjectLockDb<K,V> objectDb = writeSet.get(key);
            objectDb.setValue(value);
            return;
        }

        ObjectVersionLockDB<K,V> obj = (ObjectVersionLockDB) getKeyDatabase(key);
        if (obj == null) {
            obj = new ObjectVersionLockDBImpl<K,V>(null); // A thread fica com o write lock
            ObjectVersionLockDB<K,V> objdb = (ObjectVersionLockDB) putIfAbsent(key, obj);

            if (objdb != null)
                obj = objdb;
        }

        // o objecto esta na base de dados
        ObjectVersionLockDB<K,V> buffer = new BufferObjectVersionDB(key, obj.getValue(), obj.getVersion(), obj, false);
        buffer.setValue(value);
        addObjectDbToWriteBuffer(key, buffer);
    }

    @Override
    public boolean commit() throws TransactionTimeoutException, TransactionAbortException {
        if(!isActive)
            return success;

        Set<ObjectLockDb<K,V>> lockObjects = new HashSet<ObjectLockDb<K,V>>();

        for (ObjectVersionLockDB<K,V> buffer : writeSet.values()){
            ObjectVersionLockDB<K,V> objectDb = (ObjectVersionLockDB) buffer.getObjectDb();
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
        for (ObjectVersionLockDB<K,V> buffer : readSet.values()){ // BufferObject
            ObjectVersionLockDB<K,V> objectDb = (ObjectVersionLockDB) buffer.getObjectDb();
            if(objectDb.try_lock_read_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){

                if (buffer.getVersion() == objectDb.getVersion()) {
                    objectDb.unlock_read();
                    continue;
                } else {
                    objectDb.unlock_read();
                    abortVersions(lockObjects);
                    return false;
                }
            } else {
                abortTimeout(lockObjects);
            }
        }

        // Escrita
        for (ObjectLockDb<K,V> buffer : writeSet.values()){
            ObjectLockDb<K,V> objectDb = (ObjectLockDb) buffer.getObjectDb();
            objectDb.setValue(buffer.getValue());
        }

        commitId = Database.timestamp.getAndIncrement();
        unlockWrite_objects(lockObjects);

        isActive = false;
        success = true;
        return true;
    }

    private void abortTimeout(Set<ObjectLockDb<K,V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionTimeoutException("COMMIT: Transaction " + getId() +": "+Thread.currentThread().getName()+" - commit");
    }

    private void abortVersions(Set<ObjectLockDb<K,V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionAbortException("COMMIT: Transaction Abort " + getId() +": "+Thread.currentThread().getName()+" - Version change");
    }

    private void unlockWrite_objects(Set<ObjectLockDb<K,V>> set){
        Iterator<ObjectLockDb<K,V>> it_locks = set.iterator();
        while (it_locks.hasNext()) {
            ObjectLockDb<K,V> objectDb = it_locks.next();
            objectDb.unlock_write();
        }
    }

    @Override
    public void abort() throws TransactionAbortException{
        isActive = false;
        success = false;
        commitId = -1;
    }

    void addObjectDbToReadBuffer(K key, ObjectVersionLockDB<K,V> objectDb){
        readSet.put(key, objectDb);
    }

    void addObjectDbToWriteBuffer(K key, ObjectVersionLockDB objectDb){
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
