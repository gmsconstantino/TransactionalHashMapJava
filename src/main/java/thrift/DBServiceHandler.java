package thrift;

import fct.thesis.database.*;
import org.apache.thrift.TException;
import thrift.server.AbortException;
import thrift.server.DBService;
import thrift.server.NoSuchKeyException;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by gomes on 01/05/15.
 */
public class DBServiceHandler implements DBService.Iface {

    Database<String, Map<String, ByteBuffer>> db = DatabaseSingleton.getDatabase();
    final TransactionFactory.type TYPE = DatabaseSingleton.getInstance().getType();
    Transaction<String, Map<String, ByteBuffer>> t;

    DBServiceHandler(){
    }

    @Override
    public long txn_begin() throws org.apache.thrift.TException {
        t = db.newTransaction(TYPE);
        return t.getId();
    }

    @Override
    public boolean txn_commit() throws AbortException,TException {
        try {
            return t.commit();
        } catch(TransactionTimeoutException e){
            throw new AbortException();
        } catch (TransactionAbortException e){
            throw new AbortException();
        }
    }

    @Override
    public boolean txn_abort() throws TException {
        t.abort();
        return true;
    }

    @Override
    public Map<String,ByteBuffer> get(String key) throws AbortException,NoSuchKeyException,TException {
        try {
            Map<String,ByteBuffer> m = t.get(key);
            if (m==null)
                throw new NoSuchKeyException();
            return m;
        } catch(TransactionTimeoutException e){
            throw new AbortException();
        } catch (TransactionAbortException e){
            throw new AbortException();
        }
    }

    @Override
    public void put(String key, Map<String, ByteBuffer> value) throws AbortException,TException {
        try {
            t.put(key,value);
        } catch(TransactionTimeoutException e){
            throw new AbortException();
        } catch (TransactionAbortException e){
            throw new AbortException();
        }
    }

    @Override
    public boolean reset(String type) throws TException {
        TransactionFactory.type t = TransactionTypeFactory.getType(type);
        if (t == null)
            return false;

        DatabaseSingleton.setTransactionype(t);
        return true;
    }

}