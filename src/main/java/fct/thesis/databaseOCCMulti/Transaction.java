package fct.thesis.databaseOCCMulti;

import fct.thesis.database.*;
import fct.thesis.database2PL.Config;
import fct.thesis.databaseOCC.BufferObjectVersionDB;
import fct.thesis.databaseOCC.ObjectVersionLockDB;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K,V> extends fct.thesis.database.Transaction<K,V> {

    static AtomicInteger timestamp = new AtomicInteger(0);

    protected Map<K, Long> readSet; //set , conf se add nao altera
    protected Map<K, ObjectVersionLockDB<K,V>> writeSet;

    private long startTime;
    private long commitTime;

    public Transaction(Database db) {
        super(db);
    }

    protected synchronized void init(){
        super.init();
        readSet = new HashMap<K, Long>();
        writeSet = new HashMap<K, ObjectVersionLockDB<K,V>>();

        startTime = timestamp.get();
    }

    @Override
    public V get(K key) throws TransactionTimeoutException, TransactionAbortException {
        if (!isActive)
            return null;

        if(writeSet.containsKey(key)){
            return (V) writeSet.get(key).getValue();
        }

        V returnValue = null;

        ObjectMultiVersionLockDB<K,V> obj = (ObjectMultiVersionLockDB) getKeyDatabase(key);
        if (obj == null || obj.getVersion() == -1)
            return null;

        if(obj.try_lock_read_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){

            if (readSet.containsKey(key)){
                    returnValue = obj.getValueVersionLess(startTime);
            } else {
                addObjectDbToReadBuffer((K) key, obj.getVersion());
                returnValue = (V) obj.getValueVersionLess(startTime);
            }
        } else {
//            obj.unlock_read();
            abort();
            throw new TransactionTimeoutException("Transaction " + getId() +": Thread "+Thread.currentThread().getName()+" - get key:"+key);
        }

        obj.unlock_read();
        return returnValue;
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

        ObjectMultiVersionLockDB<K,V> obj = (ObjectMultiVersionLockDB) getKeyDatabase(key);
        //Nao existe nenhuma
        if (obj == null) {
            obj = new ObjectMultiVersionLockDB<K,V>(); // A thread fica com o write lock
            ObjectMultiVersionLockDB<K,V> objdb = (ObjectMultiVersionLockDB) putIfAbsent(key, obj);
            obj.unlock_write();

            if (objdb != null)
                obj = objdb;

            ObjectVersionLockDB<K,V> buffer = new BufferObjectVersionDB(key, obj.getValue(), obj.getVersion(), obj, true); // isNew = true
            buffer.setValue(value);
            addObjectDbToWriteBuffer(key, buffer);
            return;
        }

        // o objecto esta na base de dados
        if(obj.try_lock_read_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){
            ObjectVersionLockDB<K,V> buffer = new BufferObjectVersionDB(key, obj.getValue(), obj.getVersion(), obj, false);
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

        if (writeSet.size() == 0){
            isActive = false;
            success = true;
            return true;
        }

        Set<ObjectLockDb<K,V>> lockObjects = new HashSet<ObjectLockDb<K,V>>();

        for (ObjectVersionLockDB<K,V> buffer : writeSet.values()){
            ObjectVersionLockDB<K,V> objectDb = (ObjectVersionLockDB) buffer.getObjectDb();
            if(objectDb.try_lock_write_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){
                lockObjects.add(objectDb);

                // buffer.version == objectDb.last_version
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
        Iterator<Map.Entry<K, Long>> it = readSet.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<K, Long> obj_readSet = it.next();
            ObjectMultiVersionLockDB<K,V> objectDb = (ObjectMultiVersionLockDB) getKeyDatabase(obj_readSet.getKey());
            if(objectDb.try_lock_read_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){

                // readSet.version == objectDb.last_version
                if (obj_readSet.getValue() == objectDb.getVersion()) {
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

        commitId = Transaction.timestamp.getAndIncrement();
        // Escrita
        for (ObjectLockDb<K,V> buffer : writeSet.values()){
            ObjectMultiVersionLockDB<K,V> objectDb = (ObjectMultiVersionLockDB) buffer.getObjectDb();
            objectDb.addNewVersionObject(commitId, buffer.getValue());
        }

        unlockWrite_objects(lockObjects);

        isActive = false;
        success = true;
        return true;
    }

    private void abortTimeout(Set<ObjectLockDb<K,V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionTimeoutException("Transaction " + getId() +": Thread "+Thread.currentThread().getName()+" - commit");
    }

    private void abortVersions(Set<ObjectLockDb<K,V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionAbortException("Transaction Abort " + getId() +": Thread "+Thread.currentThread().getName()+" - Version change");
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


    void addObjectDbToReadBuffer(K key, Long version){
        readSet.put(key, version);
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
