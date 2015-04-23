package fct.thesis.database2PL;

import fct.thesis.database.*;
import pt.dct.util.P;

import java.util.*;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K,V> extends fct.thesis.database.Transaction<K,V> {

    protected Set<K> readSet;

    // Write set guarda o valor antigo que estava antes da transacao
    protected Map<K, ObjectLockDb<K,V>> writeSet;

    protected Map<K, P<Boolean,Long>> myLocks;

    public Transaction(Database db) {
        super(db);
    }

    protected void init(){
        super.init();

        readSet = new HashSet<K>();
        writeSet = new HashMap<K, ObjectLockDb<K,V>>();
        myLocks = new HashMap<K, P<Boolean,Long>>();
    }



    @Override
    public V get(K key) {
        if (!isActive)
            return null;

        if (readSet.contains(key)){
            return (V) getKeyDatabase(key).getObjectDb().getValue();
        }

        // Passa a ser um objecto do tipo ObjectLockDbImpl
        ObjectLockDbImpl<?,?> obj = (ObjectLockDbImpl) getKeyDatabase(key);

        if (obj == null)
            return null;

        long stamp = obj.try_lock_read(Config.TIMEOUT, Config.TIMEOUT_UNIT);
        if(stamp!=0){
            addObjectDbToReadBuffer((K) key);
            myLocks.put(key, new P<>(true,stamp));
            return (V) obj.getValue();
        } else {
            abort();
            throw new TransactionTimeoutException("Transaction " + getId() +": Thread "+Thread.currentThread().getName()+" - get key:"+key);
        }
    }

    @Override
    public void put(K key, V value) {
        if (!isActive)
            return;

        // Se esta na cache e pq ja tenho o write lock do objecto
        ObjectLockDb<K,V> objBuffer = getObjectFromWriteBuffer(key);
        if (objBuffer != null){
            objBuffer.setValue(value);
            return;
        }

        // Search
        ObjectLockDbImpl<K,V> obj = (ObjectLockDbImpl<K,V>) getKeyDatabase(key); // Passa a ser um objecto do tipo ObjectDbImpl

        long stamp = 0L;

        if(obj == null){
            obj = new ObjectLockDbImpl<K,V>(value);

            ObjectLockDbImpl<K,V> map_obj = (ObjectLockDbImpl<K,V>) putIfAbsent(key, obj);
            obj = map_obj!=null? map_obj : obj;
        }

        if (myLocks.containsKey(key)){
            for (int i = 0; i < 3; i++) {
                long ws = obj.try_upgrade(myLocks.get(key).s);
                if (ws != 0L) {
                    stamp = ws;
                    break;
                }
            }
            if (stamp == 0){
                abort();
                throw new TransactionTimeoutException("Transaction " + getId() +": Thread "+Thread.currentThread().getName()+" - Put key:"+key+" - Cant Upgrade Lock");
            }
        } else {
            stamp = obj.try_lock_write(Config.TIMEOUT, Config.TIMEOUT_UNIT);
        }

        if(stamp != 0){
            myLocks.put(key, new P<>(false,stamp));
            addObjectDbToWriteBuffer(key, new BufferObjectDb<K, V>(key, obj.getValue(), obj)); //No buffer o old value
            obj.setValue(value); // Escrita no objecto da bd, nao no buffer
        } else {
            abort();
            throw new TransactionTimeoutException("Transaction " + getId() +": Thread "+Thread.currentThread().getName()+" - put key:"+key+" value:"+value);
        }
    }

    @Override
    public boolean commit() {
        if (!isActive)
            return success;

//        commitId = Database.timestamp.getAndIncrement();

        Set<Map.Entry<K, ObjectLockDb<K,V>>> entrySet =  writeSet.entrySet();
        for (Map.Entry<K, ObjectLockDb<K,V>> obj : entrySet){
            ObjectLockDb objectDb = obj.getValue();
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
        Set<Map.Entry<K, ObjectLockDb<K,V>>> entrySet =  writeSet.entrySet();
        for (Map.Entry<K, ObjectLockDb<K,V>> obj : entrySet){
            ObjectLockDb objectDb = obj.getValue();
            if (objectDb.isNew()){
                removeKey(obj.getKey());
                readSet.remove(obj.getKey());
            } else {
                ((ObjectLockDb) objectDb.getObjectDb()).setValue(objectDb.getValue());
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
            ObjectLockDbImpl objectDb = (ObjectLockDbImpl) getKeyDatabase(key);
            P<Boolean,Long> lk = myLocks.get(key);
            if (lk.f)
                objectDb.unlock_read(lk.s);
        }

        for (ObjectLockDb objectDb : writeSet.values()) {
            BufferObjectDb buffer = (BufferObjectDb) objectDb;
            ObjectLockDbImpl obj = (ObjectLockDbImpl)objectDb.getObjectDb();
            P<Boolean,Long> lk = myLocks.get(buffer.getKey());
                obj.unlock_write(lk.s);
        }
    }

    void addObjectDbToReadBuffer(K key){
        readSet.add(key);
    }

    void addObjectDbToWriteBuffer(K key, ObjectLockDb obj){
        writeSet.put(key, obj);
    }

    ObjectLockDb<K,V> getObjectFromWriteBuffer(K key){
        return (ObjectLockDb) ((writeSet.get(key)!=null) ? writeSet.get(key).getObjectDb() : null);
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
