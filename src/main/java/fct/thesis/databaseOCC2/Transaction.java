package fct.thesis.databaseOCC2;

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

    protected Map<K,Long> readSet;
    protected Map<K, ObjectOCC2<K,V>> writeSet;

    public Transaction(Database db) {
        super(db);
    }

    protected void init(){
        super.init();

        readSet = new HashMap<K,Long>();
        writeSet = new HashMap<K, ObjectOCC2<K, V>>();
    }

    @Override
    public V get(K key) throws TransactionTimeoutException, TransactionAbortException {
        if (!isActive)
            return null;

        if (writeSet.containsKey(key)) {
            return (V) writeSet.get(key).getValue();
        }

        V returnValue;

        ObjectOCC2<K, V> obj = (ObjectOCC2) getKeyDatabase(key);
        if (obj == null || obj.getVersion() == -1)
            return null;

        obj.lock_read();
        if (readSet.containsKey(key)){
            if (readSet.get(key) == obj.getVersion())
                returnValue = obj.getValue();
            else {
                abort();
                throw new TransactionAbortException("GET: Transaction Abort " + getId() +": "+Thread.currentThread().getName()+" - Version change - key:"+key);
            }
        } else {
            readSet.put(key, obj.getVersion());
            returnValue = obj.getValue();
        }
        obj.unlock_read();

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

        ObjectOCC2<K,V> obj = (ObjectOCC2) getKeyDatabase(key);
        if (obj == null) {
            obj = new ObjectOCC2Impl<K,V>(null); // A thread fica com o write lock
            ObjectOCC2<K,V> objdb = (ObjectOCC2) putIfAbsent(key, obj);

            if (objdb != null)
                obj = objdb;
        }

        ObjectOCC2<K,V> buffer = new BufferOCC2(key, value, obj);
        writeSet.put(key, buffer);
        return;
    }

    @Override
    public boolean commit() throws TransactionTimeoutException, TransactionAbortException {
        if(!isActive)
            return success;

        Set<ObjectLockDb<K,V>> lockObjects = new HashSet<ObjectLockDb<K,V>>();

        for (ObjectOCC2<K,V> buffer : writeSet.values()){
            ObjectOCC2<K,V> objectDb = (ObjectOCC2) buffer.getObjectDb();
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
        for (Map.Entry<K,Long> buffer : readSet.entrySet()){ // BufferObject
            ObjectOCC2<K,V> objectDb = (ObjectOCC2) getKeyDatabase(buffer.getKey());
                if (buffer.getValue() == objectDb.getVersion()) {
                    continue;
                } else {
                    abortVersions(lockObjects);
                    return false;
                }
        }

        // Escrita
        for (ObjectLockDb<K,V> buffer : writeSet.values()){
            ObjectLockDb<K,V> objectDb = (ObjectLockDb) buffer.getObjectDb();
            objectDb.setValue(buffer.getValue());
        }

        unlockWrite_objects(lockObjects);

        isActive = false;
        success = true;
        return true;
    }

    private void abortTimeout(Set<ObjectLockDb<K,V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionTimeoutException("COMMIT: Transaction " + getId() +": Thread "+Thread.currentThread().getName()+" - commit");
    }

    private void abortVersions(Set<ObjectLockDb<K,V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionAbortException("COMMIT: Transaction Abort " + getId() +": Thread "+Thread.currentThread().getName()+" - Version change");
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
