package fct.thesis.databaseBlotter;

import fct.thesis.database.BufferObjectDb;
import fct.thesis.database.TransactionAbortException;
import fct.thesis.database.TransactionTimeoutException;
import fct.thesis.database2PL.Config;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K extends Comparable<K>,V> extends fct.thesis.database.Transaction<K,V> {

    public final static ExecutorService service = Executors.newFixedThreadPool(10);

//    private static final Logger logger = LoggerFactory.getLogger(Transaction.class);
    static AtomicLong identifier = new AtomicLong(-1L);

    Set<Long> aggStarted;
    protected Map<K, BufferObjectDb<K,V>> writeSet;

    public Transaction(fct.thesis.database.Database db) {
        super(db);
    }

    protected void init(){
        super.init();
        aggStarted = new HashSet<Long>();
        writeSet = new HashMap<K, BufferObjectDb<K,V>>();
        id = Transaction.identifier.incrementAndGet();
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

        boolean lock = false;

        Long v = obj.getVersionForTransaction(id);
        if (v==null){
            obj.lock_read();
            lock = true;
            v = obj.getLastVersion();
            obj.putSnapshot(id, v);
//            logger.debug("Tx "+id+" - Put on ["+obj+"] version "+v);
        }

        V r = obj.getValueVersion(v, aggStarted);

        if (lock)
            obj.unlock_read();

        return r;
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

        Set<ObjectBlotterDb<K,V>> lockObjects = new HashSet<ObjectBlotterDb<K,V>>();

        for (BufferObjectDb<K, V> buffer : writeSet.values()) {
//            ObjectBlotterDbImpl<K, V> objectDb = (ObjectBlotterDbImpl) buffer.getObjectDb();

            ObjectBlotterDb<K,V> objectDb = (ObjectBlotterDb) getKeyDatabase(buffer.getKey());
            //Nao existe nenhuma
            if (objectDb == null) {
                ObjectBlotterDb<K,V> obj = new ObjectBlotterDb<K,V>(); // Thread tem o lock do objecto
                ObjectBlotterDb<K,V> objdb = (ObjectBlotterDb) putIfAbsent(buffer.getKey(), obj);
                if (objdb != null) {
                    obj = null;
                    objectDb = objdb;
                }else {
                    objectDb = obj;
//                    logger.debug("Tx "+id+" - Create key="+buffer.getKey()+" object="+objectDb);
                }
            }


            if (objectDb.try_lock_write_for(Config.TIMEOUT,Config.TIMEOUT_UNIT)) {
                lockObjects.add(objectDb);
//                logger.debug("Tx "+id+" - Lock object with key " + buffer.getKey() + " object:"+objectDb);
                buffer.setObjectDb(objectDb); // Set reference to Object, para que no ciclo seguindo
                                              // nao seja necessario mais uma pesquisa no hashmap

                Long v = objectDb.getVersionForTransaction(id);
                if(v != null && v < objectDb.getLastVersion()){
                    abortVersions(lockObjects);
                    return false;
                } else {
                    // Line 22
                    aggStarted.addAll(objectDb.snapshots.keySet());
//                    logger.debug("Tx "+id+" - Add to Set: "+objectDb.snapshots.keySet());
                }
            } else {
//                logger.debug("Transaction abort because cant get Write Lock. - commit");
                abortVersions(lockObjects);
            }
        }

//        logger.debug("Tx "+id+" - Aggregated Tx Ids Set size = "+aggStarted.size());

        for (BufferObjectDb<K, V> buffer : writeSet.values()) {
            ObjectBlotterDb<K, V> objectDb = (ObjectBlotterDb) buffer.getObjectDb();

            for (Long tid : aggStarted){
//                logger.debug("Tx "+id+" - Try adding last version to Tx: "+tid+" key "+buffer.getKey()+" "+objectDb);
                if (objectDb.snapshots.get(tid) == null){
                    objectDb.putSnapshot(tid, objectDb.getLastVersion());
//                    logger.debug("Tx "+id+" - Added last version to Tx: "+ tid+" key "+buffer.getKey()+" "+objectDb);
                }
            }

            objectDb.setValue(buffer.getValue());
        }

        unlockWrite_objects(lockObjects);

        isActive = false;
        success = true;
        addToCleaner(db, id);
        return true;
    }

    private void abortVersions(Set<ObjectBlotterDb<K,V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionAbortException("Transaction Abort " + getId() +": Thread "+Thread.currentThread().getName()+" - Version change");
    }

    private void unlockWrite_objects(Set<ObjectBlotterDb<K,V>> set){
        Iterator<ObjectBlotterDb<K,V>> it_locks = set.iterator();
        while (it_locks.hasNext()) {
            ObjectBlotterDb<K,V> objectDb = it_locks.next();
            objectDb.unlock_write();
        }
    }

    @Override
    public void abort() throws TransactionAbortException{
        isActive = false;
        success = false;
        addToCleaner(db, id);
        commitId = -1;
    }

    private void addObjectDbToWriteBuffer(K key, BufferObjectDb objectDb){
        writeSet.put(key, objectDb);
    }

    public static void addToCleaner(final fct.thesis.database.Database db, final Long tid) {
        service.execute(() -> {
                try{
                    Database dbblotter = (Database) db;
                    dbblotter.addTransactiontoClean(tid);
                } catch (Exception e){}
        });
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
