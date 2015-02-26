package database;

/**
 * Created by gomes on 26/02/15.
 */
public class TransactionAbortException extends RuntimeException {

    public TransactionAbortException(String message) {
        super(message);
    }

    public TransactionAbortException() {
    }
}
