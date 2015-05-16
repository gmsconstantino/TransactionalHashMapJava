package fct.thesis.database;

/**
 * Created by gomes on 10/03/15.
 */
public class TransactionAbortException extends TransactionException {

    public TransactionAbortException() {
    }

    public TransactionAbortException(String message) {
        super(message);
    }
}
