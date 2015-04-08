package pt.dct.cli.bindings;

import java.util.HashMap;
import java.util.Map;

import pt.dct.cli.Tx;
import pt.dct.cli.TxStorage;
import pt.dct.DObject;

public class DCTStorage implements TxStorage {
	
	protected Map<String,DObject<Integer>> store;
	
	@Override
	public void init() {
		store = new HashMap<>();
	}

	@Override
	public Tx createTransaction() {
		return new DTxWrapper(this);
	}
	
	public DObject<Integer> getKey(String key) {
		return store.get(key);
	}

	
	public void createKey(String key, int val) {
		store.put(key, new DObject<Integer>(val));
	}

}
