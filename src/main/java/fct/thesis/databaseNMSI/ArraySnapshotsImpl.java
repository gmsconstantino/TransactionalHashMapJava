package fct.thesis.databaseNMSI;

import fct.thesis.database.*;
import fct.thesis.structures.MapEntry;

import java.util.*;

/**
 * Created by Constantino Gomes on 15/07/15.
 */
public class ArraySnapshotsImpl<T extends Transaction> implements SnapshotsIface<T> {

    private static final Transaction NULL_TX = new Transaction<Integer,Integer>();
    {
        NULL_TX.abort();
    }

    MapEntry<T,Long> snapshots[];

    public ArraySnapshotsImpl() {
        snapshots = new MapEntry[MAX_POS];
        for (int i=0; i < MAX_POS; i++) {
            snapshots[i] = new MapEntry<>((T) NULL_TX, null);
        }
    }

    @Override
    public Long get(T tid) {
        try {
//            int index = (int) tid.thread.getId() % MAX_POS;
            if (tid == snapshots[tid.idxThread].f) // Se o que esta no vetor e a tx actual entao ok
                return snapshots[tid.idxThread].s;
        } catch (NullPointerException e){}

        return null; // Senao deve ser algo passado
    }

    @Override
    public void put(T t, long v) {
//        int index = (int) t.thread.getId()%MAX_POS;
        snapshots[t.idxThread].f = t;
        snapshots[t.idxThread].s = v;
    }

    @Override
    public void remove(T t) {
//        if (t.isActive()){
//            int index = (int) t.thread.getId()%MAX_POS;
//            snapshots[t.idxThread].f = (T) NULL_TX;
//            snapshots[t.idxThread].s = null;
//        }
    }

    @Override
    public Iterable<Map.Entry<T, Long>> entrySet() {
        return () -> new ArrayIterator();
    }

    @Override
    public Collection<? extends Transaction> keySet() {
        ArrayList<T> keyset = new ArrayList<>(MAX_POS);
        for (int i = 0; i < MAX_POS; i++) {
            if (snapshots[i].f != NULL_TX)
                keyset.add(snapshots[i].f);
        }
        return keyset;
    }

    private class ArrayIterator implements Iterator {

        private int count;    // the number of elements in the collection
        private int current;  // the current position in the iteration

        public ArrayIterator() {
            count = snapshots.length;
            current = 0;
        }

        @Override
        public boolean hasNext() {
            for (int i = current; i < count; i++,current++) {
                if (snapshots[i].s!=null)
                    break;
            }

            return (current < count);
        }

        @Override
        public Object next() {
            if (! hasNext())
                throw new NoSuchElementException();

            return snapshots[current++];
        }

        //-----------------------------------------------------------------
        //  The remove operation is not supported in this collection.
        //-----------------------------------------------------------------
        public void remove() throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException();
        }
    }
}
