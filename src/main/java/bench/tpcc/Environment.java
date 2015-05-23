package bench.tpcc;

import bench.tpcc.server.ThriftTransaction;
import fct.thesis.database.Database;
import fct.thesis.database.DatabaseFactory;
import fct.thesis.database.TransactionFactory;
import fct.thesis.database.Transaction;
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

    public static void setTransactionype(TransactionFactory.type type){
        getInstance().setType(type);
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
    
    public static int getSizeDatabase(){
        return getInstance().db.size();
    }

    private Environment() {
//        type = TransactionFactory.type.TWOPL;
//        setType(type);
    }

    public TransactionFactory.type getType() {
        return type;
    }

    public void setType(TransactionFactory.type type) {
        this.type = type;
        System.out.println("Set new Database Transactions Type : "+ type);
        db = (Database<String , MyObject>) DatabaseFactory.createDatabase(type);
    }
}
