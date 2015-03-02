package pt.dct.cli;

public class KeyNotFoundException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public KeyNotFoundException(String key) {
		super("Key not found: "+key);
	}

}
