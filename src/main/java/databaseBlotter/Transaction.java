package databaseBlotter;

import database.*;
import database2PL.BufferObjectDb;
import database2PL.Config;
import databaseOCC.BufferObjectVersionDB;
import databaseOCC.ObjectVersionLockDB;
import databaseOCC.ObjectVersionLockDBImpl;
import databaseOCCMulti.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K,V> extends database.Transaction<K,V> {


    private static final Logger logger = LoggerFactory.getLogger(Transaction.class);
    static AtomicInteger identifier = new AtomicInteger(-1);

    Set<Long> aggStarted;
    protected Map<K, BufferObjectDb<K,V>> writeSet;

    private Long id;

    public Transaction(Database db) {
        super(db);
    }

    protected synchronized void init(){
        super.init();
        aggStarted = new HashSet<Long>();
        writeSet = new HashMap<K, BufferObjectDb<K,V>>();
        id = new Long(Transaction.identifier.incrementAndGet());
    }

    @Override
    public V get(K key) throws TransactionTimeoutException, TransactionAbortException {
        if (!isActive)
            return null;

        if(writeSet.containsKey(key)){
            return (V) writeSet.get(key).getValue();
        }

        ObjectBlotterDb<K,V> obj = (ObjectBlotterDb) getKeyDatabase(key);
        if (obj == null || obj.getLastVersion() == -1)
            return null;

        obj.lock_read();

        Long v = obj.getVersionForTransaction(id);
        if (v==null){
            v = obj.getLastVersion();
            obj.putSnapshot(id, v);
        }

        V r = obj.getValueVersion(v, aggStarted);

        obj.unlock_read();

        return r;
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
            BufferObjectDb<K,V> objectDb = writeSet.get(key);
            objectDb.setValue(value);
            return;
        }

        BufferObjectDb<K,V> buffer = new BufferObjectDb(key, value);
        buffer.setValue(value);
        addObjectDbToWriteBuffer(key, buffer);
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

//        logger.debug("Commit Write Set size: "+writeSet.size());
//        logger.debug("Commit Aggregated Set Tids size: "+aggStarted.size());

        Set<ObjectBlotterDbImpl<K,V>> lockObjects = new HashSet<ObjectBlotterDbImpl<K,V>>();

        for (BufferObjectDb<K, V> buffer : writeSet.values()) {
//            ObjectBlotterDbImpl<K, V> objectDb = (ObjectBlotterDbImpl) buffer.getObjectDb();

            ObjectBlotterDbImpl<K,V> objectDb = (ObjectBlotterDbImpl) getKeyDatabase(buffer.getKey());
            //Nao existe nenhuma
            if (objectDb == null) {
                ObjectBlotterDbImpl<K,V> obj = new ObjectBlotterDbImpl<K,V>(); // Thread tem o lock do objecto
                ObjectBlotterDbImpl<K,V> objdb = (ObjectBlotterDbImpl) putIfAbsent(buffer.getKey(), obj);
                if (objdb != null) {
                    obj = null;
                    objectDb = objdb;
                }else
                    objectDb = obj;
            }


            if (objectDb.try_lock_write_for(Config.TIMEOUT,Config.TIMEOUT_UNIT)) {
                lockObjects.add(objectDb);

                buffer.setObjectDb(objectDb); // Set reference to Object, para que no ciclo seguindo
                                              // nao seja necessario mais uma pesquisa no hashmap

                Long v = objectDb.getVersionForTransaction(id);
                if(v != null && v < objectDb.getLastVersion()){
                    abortVersions(lockObjects);
                    return false;
                } else {
                    // Line 22
                    aggStarted.addAll(objectDb.snapshots.keySet());
                }
            } else {
//                logger.debug("Transaction abort because cant get Write Lock. - commit");
                abortVersions(lockObjects);
            }
        }


        for (BufferObjectDb<K, V> buffer : writeSet.values()) {
            ObjectBlotterDbImpl<K, V> objectDb = (ObjectBlotterDbImpl) buffer.getObjectDb();

            for (Long tid : aggStarted){
                if (objectDb.snapshots.get(tid) == null)
                    objectDb.putSnapshot(tid, objectDb.getLastVersion());
            }

            objectDb.setValue(buffer.getValue());
        }

        unlockWrite_objects(lockObjects);

        isActive = false;
        success = true;
        return true;
    }

    private void abortVersions(Set<ObjectBlotterDbImpl<K,V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionAbortException("Transaction Abort " + getId() +": Thread "+Thread.currentThread().getName()+" - Version change");
    }

    private void unlockWrite_objects(Set<ObjectBlotterDbImpl<K,V>> set){
        Iterator<ObjectBlotterDbImpl<K,V>> it_locks = set.iterator();
        while (it_locks.hasNext()) {
            ObjectBlotterDbImpl<K,V> objectDb = it_locks.next();
            objectDb.unlock_write();
        }
    }

    @Override
    public void abort() throws TransactionAbortException{
        isActive = false;
        success = false;
        commitId = -1;
    }

    private void addObjectDbToWriteBuffer(K key, BufferObjectDb objectDb){
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

    public void finalize() throws Throwable {
//        System.out.println("Transaction "+id+" finalize.");

        for (BufferObjectDb<K, V> buffer : writeSet.values()){
            if (buffer.getObjectDb()!=null){
                ObjectBlotterDbImpl<K, V> objectDb = (ObjectBlotterDbImpl) buffer.getObjectDb();
                objectDb.snapshots.remove(id);
            }
        }

        super.finalize();
    }
}
