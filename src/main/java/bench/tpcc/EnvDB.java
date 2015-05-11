package bench.tpcc;

import bench.tpcc.schema.District;
import bench.tpcc.schema.Warehouse;
import fct.thesis.database.Database;
import fct.thesis.database.DatabaseFactory;
import fct.thesis.database.TransactionFactory;

import java.util.HashMap;

/**
 * Created by gomes on 11/05/15.
 */
public class EnvDB {

    TransactionFactory.type type;

    Database<String, Warehouse> db_warehouse;
    Database<String, District> db_district;
    Database<String, HashMap<String, String>> db_stock;
    Database<String, HashMap<String, String>> db_customer;
    Database<String, HashMap<String, String>> db_history;
    Database<String, HashMap<String, String>> db_order;
    Database<String, HashMap<String, String>> db_neworder;
    Database<String, HashMap<String, String>> db_orderline;

    private static EnvDB ourInstance = null;

    public static EnvDB getInstance() {
        if (ourInstance == null){
            ourInstance = new EnvDB();
        }
        return ourInstance;
    }

    public static TransactionFactory.type getTransactionType(){
        return getInstance().getType();
    }

    public static void setTransactionype(TransactionFactory.type type){
        getInstance().setType(type);
    }

    private EnvDB() {
        type = TransactionFactory.type.TWOPL;
        setType(type);
    }

    public TransactionFactory.type getType() {
        return type;
    }

    public void setType(TransactionFactory.type type) {
        this.type = type;
        System.out.println("Set new Database Transactions Type : "+ type);
        db_warehouse = (Database<String , Warehouse>) DatabaseFactory.createDatabase(type);
        db_district = (Database<String, District>) DatabaseFactory.createDatabase(type);
        db_stock = (Database<String, HashMap<String, String>>) DatabaseFactory.createDatabase(type);
    }
}
