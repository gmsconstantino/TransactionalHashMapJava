package bindings;

import database.*;
import pt.dct.cli.AbortException;
import pt.dct.cli.KeyNotFoundException;
import pt.dct.cli.Tx;

/**
 * Created by gomes on 02/03/15.
 */
public class DctTxWrapper implements Tx {

    static final TransactionFactory.type TYPE = TransactionFactory.type.BLOTTER;

    protected Database<String,Integer> db;
    protected Transaction<String,Integer> tx;
    protected DctStorage storage;

    public DctTxWrapper(DctStorage storage, Database _db) {
        this.storage = storage;
        this.db = _db;
        this.tx = db.newTransaction(TYPE);
    }

    @Override
    public boolean commit() {
        boolean b = false;
        try {
            b = tx.commit();
        } catch (TransactionTimeoutException e){

        } catch (TransactionAbortException e){

        }
        return b;
    }

    @Override
    public int read(String s) throws KeyNotFoundException, AbortException {
        try {
            Integer obj = tx.get(s);
            if (obj == null)
                throw new KeyNotFoundException(s);
            return obj;
        } catch (TransactionTimeoutException e){
//            System.out.println("error: timeout key "+s);
            throw new AbortException("timeout key "+s);
        }
    }

    @Override
    public void write(String s, int i) throws AbortException{
        try {
            tx.put(s, i);
        } catch (TransactionTimeoutException e){
            throw new AbortException("timeout key "+s);
        }
    }
}
