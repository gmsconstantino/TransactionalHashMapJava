package fct.thesis.storage;

import fct.thesis.database.Storage;
import org.mapdb.*;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Created by Constantino Gomes on 29/09/15.
 */
public class MapDBStorage<K,V> implements Storage<K,V> {

    int ntables = 0;
    DB db[] = null;
    HTreeMap<K,V> treeMap[] = null;

    public void setParameters(Properties prop) {
        ntables = Integer.parseInt(prop.getProperty("ntables"));
        db = new DB[ntables];
        treeMap = new HTreeMap[ntables];
        for (int i = 0; i < ntables; i++) {
            db[i] = DBMaker.newFileDB(new File("db/testdb_"+i))
                    .closeOnJvmShutdown()
                    .cacheLRUEnable()
                    .cacheSize(100000)
                    .make();

            // open existing an collection (or create new)
            DB.HTreeMapMaker maker = db[i].createHashMap("map");
            treeMap[i] = maker.makeOrGet();
        }

    }

    @Override
    public V getKey(int table, K key) {
        return treeMap[table].get(key);
    }

    @Override
    public V putIfAbsent(int table, K key, V obj) {
        V old = treeMap[table].putIfAbsent(key,obj);
        db[table].commit();
        return old;
    }

    @Override
    public Iterator getIterator(int table) {
        return new StorageIterator(table);
    }

    @Override
    public int getTablesNumber() {
        return ntables;
    }

    @Override
    public int getSize() {
        int total = 0;
        for (int i = 0; i < ntables; i++) {
            total += treeMap[i].size();
        }
        return total;
    }

    @Override
    public int getSizeTable(int table) {
        return treeMap[table].size();
    }

    @Override
    public void cleanup() {
        for (int i = 0; i < ntables; i++) {
            db[i].commit();
            db[i].close();
        }
    }

    private class StorageIterator implements Iterator {

        Set<Map.Entry<K, V>> set;
        Iterator<Map.Entry<K, V>> it;
        StorageIterator(int table){
            set = treeMap[table].entrySet();
            it = set.iterator();
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public Object next() {

            if(this.hasNext()){
                Map.Entry<K, V> obj = it.next();
                return obj.getValue();
            }
            return null;
        }

        @Override
        public void remove() {

        }
    }

    static int TABLE1 = 0;
    static int TABLE2 = 1;
    public static void main(String[] args) throws Exception{

        MapDBStorage storage = new MapDBStorage();
        Properties prop = new Properties();
        prop.setProperty("filePath_prefix","db/testdb");
        prop.setProperty("ntables", "2");

        storage.setParameters(prop);

        storage.putIfAbsent(TABLE1, "test", "test1");

        System.out.println(storage.getKey(TABLE1, "test"));
        System.out.println(storage.getKey(TABLE2, "test"));
    }
}
