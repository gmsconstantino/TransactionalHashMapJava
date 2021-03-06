package fct.thesis.databaseNMSI_Timeout;

import fct.thesis.database.BufferObjectDb;
import fct.thesis.database.Database;
import fct.thesis.database.TransactionAbortException;
import fct.thesis.database.TransactionTimeoutException;
import fct.thesis.database2PL.Config;
import fct.thesis.databaseNMSI.ObjectNMSIDb;

import java.util.*;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K extends Comparable<K>,V> extends fct.thesis.databaseNMSI.Transaction<K,V> {

    public Transaction(Database db) {
        super(db);
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

        Set<ObjectNMSIDb<K,V>> lockObjects = new HashSet<ObjectNMSIDb<K,V>>();

        for (BufferObjectDb<K, V> buffer : writeSet.values()) {

            ObjectNMSIDb<K,V> objectDb = (ObjectNMSIDb) getKeyDatabase(buffer.getTable(), buffer.getKey());
            //Nao existe nenhuma
            if (objectDb == null) {
                ObjectNMSIDb<K,V> obj = new ObjectNMSIDb<K,V>(); // Thread tem o lock do objecto
                obj.lock_write();

                ObjectNMSIDb<K,V> objdb = (ObjectNMSIDb) putIfAbsent(buffer.getTable(), buffer.getKey(), obj);
                if (objdb != null) {
                    obj = null;
                    objectDb = objdb;
                }else {
                    objectDb = obj;
                }
            }


            if (objectDb.try_lock_write_for(Config.TIMEOUT,Config.TIMEOUT_UNIT)) {
                lockObjects.add(objectDb);
                buffer.setObjectDb(objectDb); // Set reference to Object, para que no ciclo seguindo
                                              // nao seja necessario mais uma pesquisa no hashmap

                Long v = objectDb.getVersionForTransaction(this);
                if(v != null && v < objectDb.getLastVersion()){
                    abortVersions(lockObjects);
                    return false;
                } else {
                    // Line 22
                    aggStarted.addAll(objectDb.snapshots.keySet());
                }
            } else {
                abortVersions(lockObjects);
            }
        }

        for (BufferObjectDb<K, V> buffer : writeSet.values()) {
            ObjectNMSIDb<K, V> objectDb = (ObjectNMSIDb) buffer.getObjectDb();
            for (fct.thesis.databaseNMSI.Transaction tid : aggStarted){
                if (tid.isActive() && objectDb.snapshots.get(tid) == null){
                    objectDb.putSnapshot(tid,objectDb.getLastVersion());
                }
            }

            objectDb.setValue(buffer.getValue());
            objectDb.unlock_write();
        }

//        unlockWrite_objects(lockObjects);

        isActive = false;
        success = true;
//        addToCleaner(this);
        return true;
    }

}
