package fct.thesis.database;

/**
 * Created by gomes on 14/04/15.
 */
public class DatabaseFactory {

    public static <K,V> fct.thesis.database.Database createDatabase(TransactionFactory.type t, int ntable){
        switch (t){
            default:
                return new fct.thesis.database.Database<K,V>(ntable);
        }
    }

}
