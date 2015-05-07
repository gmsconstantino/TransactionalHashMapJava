package fct.thesis.bindings;

import bench.DB;
import bench.DBException;
import com.sun.deploy.security.ValidationState;
import fct.thesis.database.*;
import thrift.DatabaseSingleton;
import thrift.TransactionTypeFactory;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by gomes on 07/05/15.
 */
public class MicroBinding extends DB {

    Database<Integer, HashMap<String, String>> db;
    TransactionFactory.type TYPE;
    Transaction<Integer, HashMap<String, String>> t;

    public MicroBinding() {}

    public static final String ALGORITHM_DB = "algorithm";
    public static final String ALGORITHM_DB_DEFAULT = "TWOPL";


    private static final int OK = 0;
    private static final int ERROR = -1;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public void init() throws DBException {
        oneTimeInit(getProperties());
        db = DatabaseSingleton.getDatabase();
        TYPE = DatabaseSingleton.getTransactionType();
    }

    private static boolean init = true;
    private static synchronized void oneTimeInit(Properties p) {
        if (init){
            String type = p.getProperty(ALGORITHM_DB, ALGORITHM_DB_DEFAULT);
            TransactionFactory.type TYPE = TransactionTypeFactory.getType(type);
            DatabaseSingleton.setTransactionype(TYPE);
            init = false;
        }
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void cleanup() throws DBException
    {
    }

    @Override
    public UUID beginTx() {
        t = db.newTransaction(TYPE);
        return new UUID(0L,t.getId());
    }

    @Override
    public int read(Integer key, Set<String> fields, HashMap<String,String> result) {
        try {
            HashMap<String, String> v = t.get(key);

            if (v != null) {
                if (fields != null) {
                    for (String field : fields) {
                        result.put(field, v.get(field));
                    }
                } else {
                    for (String field : v.keySet()) {
                        result.put(field, v.get(field));
                    }
                }
            }

        } catch(TransactionTimeoutException e){
            return ERROR;
        } catch (TransactionAbortException e){
            return ERROR;
        }

        return OK;
    }

    @Override
    public int update(Integer key, HashMap<String,String> values) {
//        System.out.println("updatekey: " + key + " from table: " + table);

        HashMap<String,String> v = null;
        try {
            v = t.get(key);

            if (v!=null) {
                if (values != null) {
                    String value = "";
                    for (String k : values.keySet()) {
                        value = values.get(k).toString();
                        v.put(k, value);
                    }
                    t.put(key, v);
                }
            }
        } catch(TransactionTimeoutException e){
            return ERROR;
        } catch (TransactionAbortException e){
            return ERROR;
        }

        return OK;
    }

    @Override
    public int insert(Integer key, HashMap<String,String> values) {
        try {
            t.put(key, values);
        } catch(TransactionTimeoutException e){
            return ERROR;
        } catch (TransactionAbortException e){
            return ERROR;
        }
        return OK;
    }

    @Override
    public int delete(Integer key) {
        System.out.println("deletekey: " + key);
        return OK;
    }

    @Override
    public int commit(UUID txid) {
        try {
            if (t.commit())
                return OK;
            else
                return ERROR;
        } catch(TransactionTimeoutException e){
            return ERROR;
        } catch (TransactionAbortException e){
            return ERROR;
        }
    }


}
