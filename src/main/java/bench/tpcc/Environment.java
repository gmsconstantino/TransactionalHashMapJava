package bench.tpcc;

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

    public static Transaction<String, MyObject> newTransaction(){
        if (remote){
            return null;
        }else
            return getInstance().db.newTransaction(getTransactionType());
    }

    private Environment() {
        type = TransactionFactory.type.TWOPL;
        setType(type);
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