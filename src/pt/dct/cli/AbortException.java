package pt.dct.cli;

public class AbortException extends RuntimeException {

	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	public static final AbortException ABORT_EXCEPTION = new AbortException();

    public AbortException() {
    }

    public AbortException(String message) {
        super(message);
    }
}
