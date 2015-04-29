package pt.dct.cli;

public interface Tx {
	boolean commit();
	int read(String key) throws KeyNotFoundException, AbortException;
	void write(String key, int val) throws AbortException;
}
