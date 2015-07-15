package fct.thesis.databaseNMSI;

import fct.thesis.database.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by Constantino Gomes on 14/07/15.
 */
public interface SnapshotsIface<T extends fct.thesis.database.Transaction> {

    public static final int MAX_POS = 64;

    public Long get(T t);

    public void put(T t, long v);

    public void remove(T t);

    public Iterable<Map.Entry<T,Long>> entrySet();

    public Collection<? extends Transaction> keySet();
}
