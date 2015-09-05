package bench.tpcc;

import bench.tpcc.server.ThriftTransaction;
import fct.thesis.database.*;
import fct.thesis.databaseNMSI.ThreadCleanerNMSI;
import fct.thesis.databaseSI.ThreadCleanerSI;
import fct.thesis.storage.MultiHashMapStorage;
import thrift.tpcc.schema.*;

/**
 * Created by gomes on 11/05/15.
 */
public class Environment {

    TransactionFactory.type type;
    Database<String, MyObject> db;
    public static boolean remote = false;

    private static Environment ourInstance = null;

    public static Environment getInstance() {
        if (ourInstance == null){
            ourInstance = new Environment();
        }
        return ourInstance;
    }

    public static TransactionFactory.type getTransactionType(){
        return getInstance().getType();
    }

    public static void setTransactiontype(TransactionFactory.type type, int ntables){
        getInstance().setType(type, ntables);
    }

    public static void cleanup(){
        getInstance().db.cleanup();
    }

    public static Transaction<String, MyObject> newTransaction(){
        if (remote){
            return new ThriftTransaction();
        }else
            return getInstance().db.newTransaction(getTransactionType());
    }
    
    public static int getSizeDatabase(int table){
        return getInstance().db.size(table);
    }

    private Environment() {
//        type = TransactionFactory.type.TWOPL;
//        setType(type);
    }

    public TransactionFactory.type getType() {
        return type;
    }

    public void setType(TransactionFactory.type type, int ntables) {
        this.type = type;
        System.out.println("Set new Database Transactions Type : "+ type);
        db = new Database<>();
        Storage storage = new MultiHashMapStorage(ntables);
        db.setStorage(storage);
        switch (type){
            case SI:
                db.startThreadCleaner(new ThreadCleanerSI(db, db.getStorage()));
                break;
            case OCC_MULTI:
            case NMSI_ARRAY:
            case NMSI_TIMEOUT:
            case NMSI:
                db.startThreadCleaner(new ThreadCleanerNMSI<>(db, db.getStorage()));
                break;
        }
    }

}
