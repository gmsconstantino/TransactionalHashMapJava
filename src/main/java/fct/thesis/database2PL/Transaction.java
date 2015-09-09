package fct.thesis.database2PL;

import fct.thesis.database.*;
import pt.dct.util.P;

import java.util.*;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K extends Comparable<K>,V> extends TransactionAbst<K,V> {

    protected Set<P<Integer,K>> readSet;

    // Write set guarda o valor antigo que estava antes da transacao
    protected Map<K, BufferDb<K,V>> writeSet;

    protected Map<K,Long> myLocks;

    public Transaction(Database db) {
        super(db);
    }

    protected void init(){
        super.init();

        readSet = new HashSet<>();
        writeSet = new HashMap<K, BufferDb<K,V>>();
        myLocks = new HashMap<K, Long>();
    }

    @Override
    public V get(int table, K key) {
        if (!isActive)
            return null;

        if (readSet.contains(key)){
            return (V) getKeyDatabase(table, key).getObjectDb().getValue();
        }

        // Passa a ser um objecto do tipo ObjectLockDbImpl
        ObjectLock2PL<?> obj = (ObjectLock2PL) getKeyDatabase(table, key);

        if (obj == null)
            return null;

        long stamp = 0L;

        if (!myLocks.containsKey(key))
            stamp = obj.try_lock_read(Config.TIMEOUT, Config.TIMEOUT_UNIT);
        else
            stamp = myLocks.get(key); // Ja tenho o writelock, mas nao preciso se saber qual e o valor do stamp

        if(stamp!=0){
            addObjectDbToReadBuffer(table, key);
            myLocks.put(key, stamp);
            return (V) obj.getValue();
        } else {
            abort();
            throw new TransactionTimeoutException("Transaction " + getId() +": Thread "+Thread.currentThread().getName()+" - get key:"+key);
        }
    }

    @Override
    public void put(int table, K key, V value) {
        if (!isActive)
            return;

        // Se esta na cache e pq ja tenho o write lock do objecto
        ObjectLockDb<V> objBuffer = getObjectFromWriteBuffer(key);
        if (objBuffer != null){
            objBuffer.setValue(value);
            return;
        }

        // Search
        ObjectLock2PL<V> obj = (ObjectLock2PL<V>) getKeyDatabase(table, key); // Passa a ser um objecto do tipo ObjectDbImpl

        long stamp = 0L;

        if(obj == null){
            obj = new ObjectLock2PL<V>(value);

            ObjectLock2PL<V> map_obj = (ObjectLock2PL<V>) putIfAbsent(table, key, obj);
            obj = map_obj!=null? map_obj : obj;
        }

        if (myLocks.containsKey(key)){
            for (int i = 0; i < 3; i++) {
                long ws = obj.try_upgrade(myLocks.get(key));
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
            myLocks.put(key, stamp);
            addObjectDbToWriteBuffer(key, new BufferObjectDb<K, V>(table, key, obj.getValue(), obj)); //No buffer o old value
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

        for (Map.Entry<K, BufferDb<K,V>> entry : writeSet.entrySet()){
            ObjectLock2PL objectDb = (ObjectLock2PL) entry.getValue().getObjectDb();
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
        for (Map.Entry<K, BufferDb<K,V>> entry : writeSet.entrySet()){
            ObjectLock2PL objectDb = (ObjectLock2PL) entry.getValue().getObjectDb();
            if (objectDb.isNew()){
                removeKey(entry.getValue().getTable(), entry.getKey());
                readSet.remove(entry.getKey());
            } else {
                objectDb.setValue(entry.getValue().getValue()); // Second getValue is from buffer object, para repor o valor antes da transacao
            }
        }

        unlock_locks();
        isActive = false;
        success = false;
    }

    private void unlock_locks(){
        Iterator<P<Integer,K>> it_keys = readSet.iterator();
        while (it_keys.hasNext()) {
            P<Integer,K> p = it_keys.next();
            ObjectLock2PL objectDb = (ObjectLock2PL) getKeyDatabase(p.f, p.s);
            objectDb.unlock(myLocks.get(p.s));
            myLocks.remove(p.f, p.s);
        }

        for (BufferDb<K,V> bufferDb : writeSet.values()) {
            ObjectLock2PL obj = (ObjectLock2PL)bufferDb.getObjectDb();

            if(myLocks.containsKey(bufferDb.getKey()))
                obj.unlock_write(myLocks.get(bufferDb.getKey()));
        }
    }

    void addObjectDbToReadBuffer(int table, K key){
        readSet.add(new P<>(table,key));
    }

    void addObjectDbToWriteBuffer(K key, BufferDb obj){
        writeSet.put(key, obj);
    }

    ObjectLockDb<V> getObjectFromWriteBuffer(K key){
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
