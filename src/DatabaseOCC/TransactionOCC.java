package databaseOCC;

import database2PL.Config;
import database.*;

import java.util.*;

/**
 * Created by gomes on 26/02/15.
 */

public class TransactionOCC<K,V> extends Transaction<K,V> {

    protected Map<K, ObjectDb<K,V>> readSet; //set , conf se add nao altera
    protected Map<K, ObjectDb<K,V>> writeSet;

    public TransactionOCC(Database db) {
        super(db);
    }

    protected synchronized void init(){
        super.init();

        readSet = new HashMap<K, ObjectDb<K,V>>();
        writeSet = new HashMap<K, ObjectDb<K,V>>();
    }

    @Override
    public V get(K key) throws TransactionTimeoutException {
        if (!isActive)
            return null;

        if(writeSet.containsKey(key)){
//            if (readSet.containsKey(key)){
//                readSet.remove(key);
//            }
            return (V) writeSet.get(key).getValue();
        } else if (readSet.containsKey(key)){
            // ir a BD,
            return (V) readSet.get(key).getValue();
        }

        ObjectDb<?,?> obj = getKeyDatabase(key);
        if (obj == null)
            return null;

        if(obj.try_lock_read_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){
            addObjectDbToReadBuffer((K) key, new BufferObjectDb(key, obj.getValue(), obj.getVersion(), obj, false)); // isNew = false
            V value = (V) obj.getValue();
            obj.unlock_read();
            return value;
        } else {
            abort();
            throw new TransactionTimeoutException("Transaction " + getId() +": Thread "+Thread.currentThread().getName()+" - get key:"+key);
        }
    }

    @Override
    public V get_to_update(K key) throws TransactionTimeoutException{
        return get(key);
    }

    @Override
    public void put(K key, V value) throws TransactionTimeoutException{
        if(!isActive)
            return;

        if (readSet.containsKey(key)){ // O objecto buffer muda para o writeset com o novo valor
            ObjectDb<K,V> objectDb = readSet.get(key);
            readSet.remove(key);

            objectDb.setValue(value);
            writeSet.put(key, objectDb);
            return;
        }

        if(writeSet.containsKey(key)){
            ObjectDb<K,V> objectDb = writeSet.get(key);
            objectDb.setValue(value);
            return;
        }

        ObjectDb<?,?> obj = getKeyDatabase(key);
        if (obj == null) {
            obj = new ObjectDbImpl<K,V>(value); // A thread fica com o write lock
//            db.putIfAbsent(key, null);
            addObjectDbToWriteBuffer(key, new BufferObjectDb(key, obj.getValue(), obj.getVersion(), obj, true)); // isNew = true
            obj.unlock_write();
            return;
        }

        // o objecto esta na base de dados
        if(obj.try_lock_read_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){
            ObjectDb<K,V> buffer = new BufferObjectDb(key, obj.getValue(), obj.getVersion(), obj, false);
            buffer.setValue(value);
            addObjectDbToWriteBuffer((K) key, buffer);
            obj.unlock_read();
        } else {
            abort();
            throw new TransactionTimeoutException("Transaction " + getId() +": Thread "+Thread.currentThread().getName()+" - get key:"+key);
        }
    }

    @Override
    public boolean commit() throws TransactionTimeoutException{
        if(!isActive)
            return success;

        Set<ObjectDb<K,V>> lockObjects = new HashSet<>();

        for (ObjectDb<K,V> buffer : writeSet.values()){
            ObjectDb<K,V> objectDb = buffer.getObjectDb();
            if(objectDb.try_lock_write_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){
                lockObjects.add(objectDb);

                if (buffer.isNew()) {
                    ObjectDb<K, V> map_obj = putIfAbsent(buffer.getKey(), buffer.getObjectDb());
                    if (map_obj != null){
                        unlockWrite_objects(lockObjects);
                        abort();
                        return false;
                    }
                }

                if (buffer.getVersion() == objectDb.getVersion())
                    continue;
                else {
                    abort();
                    return false;
                }
            } else {
                abortTimeout(lockObjects);
                return false;
            }
        }

        // Validate Read Set
        for (ObjectDb<K,V> buffer : readSet.values()){ // BufferObject
            ObjectDb<K,V> objectDb = buffer.getObjectDb();
            if(objectDb.try_lock_read_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){

                if (buffer.getVersion() == objectDb.getVersion()) {
                    objectDb.unlock_read();
                    continue;
                } else {
                    abort();
                    return false;
                }
            } else {
                abortTimeout(lockObjects);
            }
        }

        // Escrita
        for (ObjectDb<K,V> buffer : writeSet.values()){
            if (buffer.isNew()) {
                ObjectDb<K, V> map_obj = putIfAbsent(buffer.getKey(), buffer.getObjectDb());
                if (map_obj != null){
                    unlockWrite_objects(lockObjects);
                    abort();
                    return false;
                }
            }
            ObjectDb<K,V> objectDb = buffer.getObjectDb();
            objectDb.setValue(buffer.getValue());
        }

        commitId = Database.timestamp.getAndIncrement();
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

    private void unlockWrite_objects(Set<ObjectDb<K,V>> set){
        Iterator<ObjectDb<K,V>> it_locks = set.iterator();
        while (it_locks.hasNext()) {
            ObjectDb<K,V> objectDb = it_locks.next();
            objectDb.unlock_write();
        }
    }

    @Override
    public void abort() throws TransactionTimeoutException{
        isActive = false;
        success = false;
        return;
    }

    void addObjectDbToReadBuffer(K key, ObjectDb objectDb){
        readSet.put(key, objectDb);
    }

    void addObjectDbToWriteBuffer(K key, ObjectDb objectDb){
        writeSet.put(key, objectDb);
    }

    ObjectDb<?,?> getObjectFromReadBuffer(K key){
        return (readSet.get(key)!=null) ? readSet.get(key).getObjectDb() : null;
    }

    ObjectDb<?,?> getObjectFromWriteBuffer(K key){
        return (writeSet.get(key)!=null) ? writeSet.get(key).getObjectDb() : null;
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
