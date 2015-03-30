package databaseBlotter;

import database.*;
import database2PL.Config;
import databaseOCC.BufferObjectVersionDB;
import databaseOCC.ObjectVersionDB;
import databaseOCCMulti.ObjectMultiVersionDB;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K,V> extends database.Transaction<K,V> {

    static AtomicInteger identifier = new AtomicInteger(0);

    protected Map<K, ObjectVersionDB<K,V>> writeSet;

    private long id;
    private long commitTime;

    public Transaction(Database db) {
        super(db);
    }

    protected synchronized void init(){
        super.init();
        writeSet = new HashMap<K, ObjectVersionDB<K,V>>();
        id = Transaction.identifier.get();
    }

    @Override
    public V get(K key) throws TransactionTimeoutException, TransactionAbortException {
        if (!isActive)
            return null;

        if(writeSet.containsKey(key)){
            return (V) writeSet.get(key).getValue();
        }

        V returnValue = null;

        ObjectMultiVersionDB<K,V> obj = (ObjectMultiVersionDB) getKeyDatabase(key);
        if (obj == null || obj.getVersion() == -1)
            return null;

        if(obj.try_lock_read_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){
            returnValue = obj.getValueVersionLess(id);
        } else {
            obj.unlock_read();
            abort();
            throw new TransactionTimeoutException("Transaction " + getId() +": Thread "+Thread.currentThread().getName()+" - get key:"+key);
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
            ObjectDb<K,V> objectDb = writeSet.get(key);
            objectDb.setValue(value);
            return;
        }

        ObjectMultiVersionDB<K,V> obj = (ObjectMultiVersionDB) getKeyDatabase(key);
        //Nao existe nenhuma
        if (obj == null) {
            obj = new ObjectMultiVersionDB<>(); // A thread fica com o write lock
            ObjectMultiVersionDB<K,V> objdb = (ObjectMultiVersionDB) putIfAbsent(key, obj);
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

        if (writeSet.size() == 0){
            isActive = false;
            success = true;
            return true;
        }

        Set<ObjectDb<K,V>> lockObjects = new HashSet<>();

        for (ObjectVersionDB<K,V> buffer : writeSet.values()){
            ObjectVersionDB<K,V> objectDb = (ObjectVersionDB) buffer.getObjectDb();
            if(objectDb.try_lock_write_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){
                lockObjects.add(objectDb);

                // buffer.version == objectDb.last_version
                if (objectDb.getVersion() < id) {
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


        commitId = Transaction.identifier.getAndIncrement();
        // Escrita
        for (ObjectDb<K,V> buffer : writeSet.values()){
            ObjectMultiVersionDB<K,V> objectDb = (ObjectMultiVersionDB) buffer.getObjectDb();
            objectDb.addNewVersionObject(commitId, buffer.getValue());
        }

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

    void addObjectDbToWriteBuffer(K key, ObjectVersionDB objectDb){
        writeSet.put(key, objectDb);
    }


    @Override
    public String toString() {
        return "Transaction{" +
                "id=" + id +
                ", writeSet=" + writeSet +
                ", isActive=" + isActive +
                ", success=" + success +
                '}';
    }

}
