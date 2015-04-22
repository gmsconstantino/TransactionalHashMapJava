package fct.thesis.bindings;

import fct.thesis.database.Database;
import fct.thesis.database.DatabaseFactory;
import fct.thesis.database.TransactionFactory;
import pt.dct.cli.Tx;
import pt.dct.cli.TxStorage;

/**
 * Created by gomes on 02/03/15.
 */
public class DctStorage implements TxStorage {

    static final TransactionFactory.type TYPE = TransactionFactory.type.TWOPL;
    protected Database<Integer,Integer> db;

    @Override
    public void init() {
        db = (Database<Integer,Integer>) DatabaseFactory.createDatabase(TYPE);
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
