package pt.dct.cli;

public interface TxStorage {
	void init();
	Tx createTransaction();
	
}
