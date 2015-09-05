package fct.thesis.bindings;

import fct.thesis.database.*;
import fct.thesis.databaseNMSI.ThreadCleanerNMSI;
import fct.thesis.databaseSI.ThreadCleanerSI;
import fct.thesis.storage.MultiHashMapStorage;
import pt.dct.cli.Tx;
import pt.dct.cli.TxStorage;

/**
 * Created by gomes on 02/03/15.
 */
public class DctStorage implements TxStorage {

    private static final int TABLE = 1;

    static final TransactionFactory.type TYPE = TransactionFactory.type.NMSI;
    protected Database<String,Integer> db;
    protected Storage storage;

    @Override
    public void init() {
        db= new Database<>();
        storage = new MultiHashMapStorage();
        db.setStorage(storage);
        switch (TYPE){
            case SI:
                db.startThreadCleaner(new ThreadCleanerSI(db, db.getStorage()));
                break;
            case OCC_MULTI:
            case NMSI:
            case NMSI_ARRAY:
            case NMSI_TIMEOUT:
                db.startThreadCleaner(new ThreadCleanerNMSI<>(db, db.getStorage()));
                break;
        }
//        storage = db.getStorage();
    }

    @Override
    public Tx createTransaction() {
        return new DctTxWrapper(this,db);
    }

//    public DObject<Integer> getKey(String key) {
//        return store.get(key);
//    }
//
//
//    public void createKey(String key, int val) {
//        store.put(key, new DObject<Integer>(val));
//    }

}
