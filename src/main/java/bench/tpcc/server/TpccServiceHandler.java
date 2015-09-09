package bench.tpcc.server;

import bench.tpcc.Environment;
import fct.thesis.database.TransactionAbst;
import fct.thesis.database.TransactionAbortException;
import fct.thesis.database.TransactionFactory;
import fct.thesis.database.TransactionTimeoutException;
import org.apache.thrift.TException;
import fct.thesis.database.TransactionTypeFactory;
import thrift.server.AbortException;
import thrift.server.NoSuchKeyException;
import thrift.tpcc.TpccService;
import thrift.tpcc.schema.TObject;

/**
 * Created by gomes on 01/05/15.
 */
public class TpccServiceHandler implements TpccService.Iface {

    TransactionAbst<String, TObject> t;

    TpccServiceHandler(){
    }

    @Override
    public long txn_begin() throws TException {
        t = Environment.newTransaction();
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
    public TObject get(int table, String key) throws AbortException,NoSuchKeyException,TException {
        try {
            TObject m = t.get(table, key);
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
    public void put(int table, String key, TObject value) throws AbortException,TException {
        try {
            t.put(table, key,value);
        } catch(TransactionTimeoutException e){
            throw new AbortException();
        } catch (TransactionAbortException e){
            throw new AbortException();
        }
    }

    @Override
    public boolean reset(String type, int ntables) throws TException {
        TransactionFactory.type t = TransactionTypeFactory.getType(type);
        if (t == null)
            return false;

        Environment.setTransactiontype(t, ntables);
        return true;
    }

    @Override
    public boolean shutdown() throws TException {
        System.out.println("Stopping the Database server...");
        TpccServer.server.stop();
        return TpccServer.server.isServing();
    }

}
