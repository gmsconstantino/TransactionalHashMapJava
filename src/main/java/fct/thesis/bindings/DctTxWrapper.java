package fct.thesis.bindings;

import fct.thesis.database.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.dct.cli.AbortException;
import pt.dct.cli.KeyNotFoundException;
import pt.dct.cli.Tx;

/**
 * Created by gomes on 02/03/15.
 */
public class DctTxWrapper implements Tx {

    private static final Logger logger = LoggerFactory.getLogger(DctTxWrapper.class);

    protected Database<String,Integer> db;
    protected Transaction<String,Integer> tx;
    protected DctStorage storage;

    public DctTxWrapper(DctStorage storage, Database _db) {
        this.storage = storage;
        this.db = _db;
        this.tx = db.newTransaction(DctStorage.TYPE);
    }

    @Override
    public boolean commit() {
        boolean b = false;
        try {
            b = tx.commit();
        } catch (TransactionTimeoutException e){
            logger.debug("Commit Transaction Timeout",e);
        } catch (TransactionAbortException e){
            logger.debug("Commit Transaction Abort",e);
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
            logger.debug("Read Transaction Timeout",e);
            throw new AbortException("timeout key "+s);
        }
    }

    @Override
    public void write(String s, int i) throws AbortException{
        try {
            tx.put(s, i);
        } catch (TransactionTimeoutException e){
            logger.debug("Write Transaction Timeout",e);
            throw new AbortException("timeout key "+s);
        }
    }
}
