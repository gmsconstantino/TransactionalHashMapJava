package pt.dct.cli.bindings;

import pt.dct.DObject;
import pt.dct.DTx;
import pt.dct.cli.KeyNotFoundException;
import pt.dct.cli.Tx;

public class DTxWrapper implements Tx {

	protected DTx tx;
	protected DCTStorage storage;
	
	public DTxWrapper(DCTStorage storage) {
		this.storage = storage;
		this.tx = new DTx();
		this.tx.start();
	}
	
	@Override
	public boolean commit() {
		return tx.commit();
	}

	@Override
	public int read(String key) throws KeyNotFoundException {
		DObject<Integer> cell = storage.getKey(key);
		if (cell == null) {
			throw new KeyNotFoundException(key);
		}
		return tx.read(storage.getKey(key));
	}

	@Override
	public void write(String key, int val) {
		DObject<Integer> cell = storage.getKey(key);
		if (cell == null) {
			storage.createKey(key, val);
		}
		tx.write(storage.getKey(key), val);
	}
	
}
