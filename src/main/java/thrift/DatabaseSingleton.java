package thrift;

import fct.thesis.database.Database;
import fct.thesis.database.DatabaseFactory;
import fct.thesis.database.TransactionFactory;

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

    public static void setTransactionype(TransactionFactory.type type, int ntables){
        getInstance().setType(type, ntables);
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

    public void setType(TransactionFactory.type type, int ntables) {
        this.type = type;
        db = (Database<String, HashMap<String, ByteBuffer>>) DatabaseFactory.createDatabase(type, ntables);
        System.out.println("Set new Database Transactions Type : "+ type);
    }
}
