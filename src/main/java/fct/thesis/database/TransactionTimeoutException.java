package fct.thesis.database;

/**
 * Created by gomes on 26/02/15.
 */
public class TransactionTimeoutException extends TransactionException {

    public TransactionTimeoutException(String message) {
        super(message);
    }

    public TransactionTimeoutException() {
    }
}
