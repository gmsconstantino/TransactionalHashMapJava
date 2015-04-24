package fct.thesis.databaseOCC;

import fct.thesis.database.*;
import fct.thesis.database2PL.Config;
import fct.thesis.structures.MapEntry;

import java.util.*;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K,V> extends fct.thesis.database.Transaction<K,V> {

    protected Map<K, BufferDb<K,V>> readSet;
    protected Map<K, BufferDb<K,V>> writeSet;

    public Transaction(Database db) {
        super(db);
    }

    protected void init(){
        super.init();

        readSet = new TreeMap<K, BufferDb<K,V>>();
        writeSet = new TreeMap<K, BufferDb<K,V>>();
    }

    @Override
    public V get(K key) throws TransactionTimeoutException, TransactionAbortException {
        if (!isActive)
            return null;

        if(writeSet.containsKey(key)){
            return (V) writeSet.get(key).getValue();
        }

        V returnValue;
        long versionObj;

        ObjectLockOCC<K,V> obj = (ObjectLockOCC) getKeyDatabase(key);
        if (obj == null || obj.getVersion() == -1)
            return null;

        long stamp = obj.tryOptimisticRead();
        returnValue = obj.getValue();
        versionObj = obj.getVersion();
        if (!obj.validate(stamp)) {
            stamp = obj.lockStamp_read();
            try {
                returnValue = obj.getValue();
                versionObj = obj.getVersion();
            } finally {
                obj.unlock(stamp);
            }
        }

        if (readSet.containsKey(key)){
            if (versionObj != readSet.get(key).getVersion()) {
                abort();
                throw new TransactionAbortException("GET: Transaction Abort " + getId() + ": " + Thread.currentThread().getName() + " - Version change - key:" + key);
            }
        } else {
            addObjectDbToReadBuffer(key, new BufferObjectDb(key, returnValue, versionObj, obj));
        }

        return returnValue;
    }

    @Override
    public void put(K key, V value) throws TransactionTimeoutException{
        if(!isActive)
            return;

        if(writeSet.containsKey(key)){
            writeSet.get(key).setValue(value); // set new value on buffer
            return;
        }

        ObjectLockOCC<K,V> obj = (ObjectLockOCC) getKeyDatabase(key);
        if (obj == null) {
            obj = new ObjectLockOCC<K,V>(null); // A thread fica com o write lock
            ObjectLockOCC<K,V> objdb = (ObjectLockOCC) putIfAbsent(key, obj);

            if (objdb != null)
                obj = objdb;
        }

        // o objecto esta na base de dados
        BufferObjectDb<K,V> buffer = new BufferObjectDb(key, value, obj.getVersion(), obj);
        addObjectDbToWriteBuffer(key, buffer);
    }

    @Override
    public boolean commit() throws TransactionTimeoutException, TransactionAbortException {
        if(!isActive)
            return success;

        Map<ObjectLockOCC<K,V>,Long> lockObjects = new HashMap<ObjectLockOCC<K,V>,Long>();
        long stamp = 0;

        for (BufferDb<K,V> buffer : writeSet.values()){
            ObjectLockOCC<K,V> objectDb = (ObjectLockOCC) buffer.getObjectDb();

            stamp = objectDb.lockStamp_write();
            lockObjects.put(objectDb, stamp);

            if (buffer.getVersion() == objectDb.getVersion()) {
                continue;
            } else {
                abortVersions(lockObjects);
                return false;
            }
        }


        // Validate Read Set
        for (BufferDb<K,V> buffer : readSet.values()){ // BufferObject
            ObjectLockOCC<K,V> objectDb = (ObjectLockOCC) buffer.getObjectDb();

            stamp = objectDb.tryOptimisticRead();
            if (buffer.getVersion() == objectDb.getVersion())
                continue;
            if (!objectDb.validate(stamp)) {
                objectDb.lock_read();

                if (buffer.getVersion() == objectDb.getVersion()) {
                    objectDb.unlock_read();
                } else {
                    objectDb.unlock_read();
                    abortVersions(lockObjects);
                    return false;
                }
            }
        }

        commitId = Database.timestamp.getAndIncrement();

        // Escrita
        for (BufferDb<K,V> buffer : writeSet.values()){
            ObjectLockOCC<K,V> objectDb = (ObjectLockOCC) buffer.getObjectDb();
            objectDb.setValue(buffer.getValue());
            objectDb.unlock(lockObjects.get(objectDb));
        }

        isActive = false;
        success = true;
        return true;
    }

    private void abortTimeout(Map<ObjectLockOCC<K,V>,Long> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionTimeoutException("COMMIT: Transaction " + getId() +": "+Thread.currentThread().getName()+" - commit");
    }

    private void abortVersions(Map<ObjectLockOCC<K,V>,Long> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionAbortException("COMMIT: Transaction Abort " + getId() +": "+Thread.currentThread().getName()+" - Version change");
    }

    private void unlockWrite_objects(Map<ObjectLockOCC<K,V>,Long> set){
        for (Map.Entry<ObjectLockOCC<K,V>,Long> entry : set.entrySet()){
            entry.getKey().unlock(entry.getValue());
        }
    }

    @Override
    public void abort() throws TransactionAbortException{
        isActive = false;
        success = false;
        commitId = -1;
    }

    void addObjectDbToReadBuffer(K key, BufferDb<K,V> objectDb){
        readSet.put(key, objectDb);
    }

    void addObjectDbToWriteBuffer(K key, BufferDb objectDb){
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
