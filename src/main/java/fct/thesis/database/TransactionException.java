package fct.thesis.database;

/**
 * Created by gomes on 10/03/15.
 */
public class TransactionException extends RuntimeException {

    public TransactionException() {
    }

    public TransactionException(String message) {
        super(message);
    }
}
