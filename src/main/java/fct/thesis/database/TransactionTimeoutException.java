package fct.thesis.database;

/**
 * Created by gomes on 26/02/15.
 */
public class TransactionTimeoutException extends RuntimeException {

    public TransactionTimeoutException(String message) {
        super(message);
    }

    public TransactionTimeoutException() {
    }
}
