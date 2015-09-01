package thrift;

import fct.thesis.database.*;
import fct.thesis.databaseNMSI.ThreadCleanerNMSI;
import fct.thesis.databaseSI.ThreadCleanerSI;
import fct.thesis.storage.MultiHashMapStorage;

import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * Created by gomes on 26/03/15.
 */
public class DatabaseSingleton {

    Database<String, HashMap<String, ByteBuffer>> db;
    TransactionFactory.type type;

    private static DatabaseSingleton ourInstance = null;

    public static DatabaseSingleton getInstance() {
        if (ourInstance == null){
            ourInstance = new DatabaseSingleton();
        }
        return ourInstance;
    }

    public static Database getDatabase() {
        return getInstance().getDb();
    }

    public static TransactionFactory.type getTransactionType(){
        return getInstance().getType();
    }

    public static void setTransactionype(TransactionFactory.type type){
        getInstance().setType(type);
    }

    public static void setStorage(Storage st){
        getInstance()._setStorage(st);
    }

    private DatabaseSingleton() {
        type = TransactionFactory.type.TWOPL;
    }

    private Database<String, HashMap<String, ByteBuffer>> getDb() {
        return db;
    }

    public TransactionFactory.type getType() {
        return type;
    }

    public void setType(TransactionFactory.type type) {
        this.type = type;
        db = new Database<>();
        Storage<Integer,Integer> storage = new MultiHashMapStorage<>();
        db.setStorage(storage);
        switch (type){
            case SI:
                db.startThreadCleaner(new ThreadCleanerSI(db, storage));
                break;
            case OCC_MULTI:
            case NMSI:
                db.startThreadCleaner(new ThreadCleanerNMSI<>(db, storage));
                break;
        }
        System.out.println("Set new Database Transactions Type : "+ type);
    }

    private void _setStorage(Storage st){
        db.setStorage(st);
    }
}
